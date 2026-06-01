/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/version"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

// processorLogLevel returns LOG_LEVEL for processor pods (from env PROCESSOR_LOG_LEVEL or default "info").
func processorLogLevel() string {
	if v := os.Getenv("PROCESSOR_LOG_LEVEL"); v != "" {
		return v
	}
	return "info"
}

// processorProgressTimeoutSeconds returns PROCESSOR_PROGRESS_TIMEOUT_SECONDS for processor pods
// (from operator env PROCESSOR_PROGRESS_TIMEOUT_SECONDS or default "600").
func processorProgressTimeoutSeconds() string {
	if v := os.Getenv("PROCESSOR_PROGRESS_TIMEOUT_SECONDS"); v != "" {
		return v
	}
	return "600"
}

// processorStartupProbe returns the startup probe for processor containers (/readyz).
func processorStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intstr.FromInt(9090),
			},
		},
		PeriodSeconds:    10,
		TimeoutSeconds:   5,
		FailureThreshold: 120,
		SuccessThreshold: 1,
	}
}

// processorLivenessProbe returns the liveness probe for processor containers (/livez).
// Fails when the pipeline has no progress for PROCESSOR_PROGRESS_TIMEOUT_SECONDS (~3 failures × 30s).
func processorLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/livez",
				Port: intstr.FromInt(9090),
			},
		},
		PeriodSeconds:    30,
		TimeoutSeconds:   5,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}
}

// processorSentryEnvVars returns env vars for Sentry to pass to processor pods.
// When SENTRY_DSN is set in the operator, these vars are forwarded so processors can report to Sentry.
func processorSentryEnvVars() []corev1.EnvVar {
	if os.Getenv("SENTRY_DSN") == "" {
		return nil
	}
	var out []corev1.EnvVar
	for _, key := range []string{"SENTRY_DSN", "SENTRY_ENVIRONMENT", "SENTRY_TRACES_SAMPLE_RATE", "SENTRY_DEBUG", "SENTRY_RELEASE"} {
		if v := os.Getenv(key); v != "" {
			out = append(out, corev1.EnvVar{Name: key, Value: v})
		}
	}
	return out
}

// processorServiceAccountFor returns the dedicated ServiceAccount for the processor when checkpoint persistence
// is enabled or when the Nessie sink uses S3 Secrets in the DataFlow namespace (secretKeyRef requires Secret get).
func (r *DataFlowReconciler) processorServiceAccountFor(dataflow *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) string {
	if resolvedSpec == nil {
		return ""
	}
	if resolvedSpec.CheckpointPersistence == nil || *resolvedSpec.CheckpointPersistence {
		return k8snames.ProcessorServiceAccount(dataflow.Name)
	}
	if nessieSinkUsesLocalObjectStorageSecretRefs(&resolvedSpec.Sink, dataflow.Namespace) {
		return k8snames.ProcessorServiceAccount(dataflow.Name)
	}
	return ""
}

// processorImageFor returns the container image to use for the dataflow processor.
// Precedence: spec.ProcessorImage > spec.ProcessorVersion (repo+tag) > default (same as controller).
func (r *DataFlowReconciler) processorImageFor(dataflow *dataflowv1.DataFlow) string {
	if dataflow.Spec.ProcessorImage != "" {
		return dataflow.Spec.ProcessorImage
	}
	if dataflow.Spec.ProcessorVersion != "" {
		return version.ProcessorImageWithTag(dataflow.Spec.ProcessorVersion)
	}
	return r.processorImage
}

// specHashAnnotation is the pod template annotation key for spec content hash.
// When spec changes, the hash changes, triggering a Deployment rollout.
const specHashAnnotation = "dataflow.dataflow.io/spec-hash"

// createOrUpdateDeployment creates or updates Deployment for the processor.
// Uses a dedicated ServiceAccount when checkpoint persistence is enabled or Nessie sink uses same-namespace S3 Secret refs.
func (r *DataFlowReconciler) createOrUpdateDeployment(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) error {
	log := log.FromContext(ctx)

	deploymentName := k8snames.ProcessorDeployment(dataflow.Name)
	configMapName := k8snames.ProcessorSpecConfigMap(dataflow.Name)

	// Compute spec hash so pod template changes when ConfigMap content changes, triggering rollout.
	specJSON, err := json.Marshal(resolvedSpec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec for hash: %w", err)
	}
	hash := sha256.Sum256(specJSON)
	specHash := hex.EncodeToString(hash[:])

	labels := map[string]string{
		"app":                        "dataflow-processor",
		"dataflow.dataflow.io/name":  dataflow.Name,
		"dataflow.dataflow.io/owned": "true",
	}

	processorImage := r.processorImageFor(dataflow)

	processorEnv := append(
		[]corev1.EnvVar{
			{Name: "LOG_LEVEL", Value: processorLogLevel()},
			{Name: "PROCESSOR_PROGRESS_TIMEOUT_SECONDS", Value: processorProgressTimeoutSeconds()},
		},
		processorSentryEnvVars()...,
	)
	if resolvedSpec != nil {
		if cfg, err := resolvedSpec.Sink.GetNessieConfig(); err == nil && cfg != nil {
			s3Env, err := nessieSinkObjectStorageEnvWithResolve(ctx, r.secretResolver, req.Namespace, cfg)
			if err != nil {
				return fmt.Errorf("nessie sink object storage env: %w", err)
			}
			processorEnv = append(processorEnv, s3Env...)
		}
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: req.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: func() *int32 { n := desiredProcessorReplicas(resolvedSpec); return &n }(),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						specHashAnnotation: specHash,
					},
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr.To(int64(600)),
					ServiceAccountName:            r.processorServiceAccountFor(dataflow, resolvedSpec),
					Containers: []corev1.Container{
						{
							Name:  "processor",
							Image: processorImage,
							Command: []string{
								"/processor",
								"--spec-path=/etc/dataflow/spec.json",
								"--namespace=" + req.Namespace,
								"--name=" + dataflow.Name,
							},
							Env: processorEnv,
							Ports: []corev1.ContainerPort{
								{Name: "metrics", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
							},
							StartupProbe:  processorStartupProbe(),
							LivenessProbe: processorLivenessProbe(),
							Lifecycle: &corev1.Lifecycle{
								PreStop: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "-c", "sleep 5"},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "spec",
									MountPath: "/etc/dataflow",
									ReadOnly:  true,
								},
							},
							Resources: r.getResourceRequirements(dataflow),
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "spec",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configMapName,
									},
								},
							},
						},
					},
					ImagePullSecrets: dataflow.Spec.ImagePullSecrets,
					NodeSelector:     dataflow.Spec.NodeSelector,
					Affinity:         dataflow.Spec.Affinity,
					Tolerations:      dataflow.Spec.Tolerations,
				},
			},
		},
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(dataflow, deployment, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if Deployment exists
	existing := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: req.Namespace}, existing)
	if err != nil && apierrors.IsNotFound(err) {
		// Ensure finalizer before creating first child so deletion is coordinated
		if err := r.ensureDataFlowFinalizer(ctx, req); err != nil {
			return fmt.Errorf("failed to ensure DataFlow finalizer: %w", err)
		}
		// Create new Deployment
		if err := r.Create(ctx, deployment); err != nil {
			return fmt.Errorf("failed to create Deployment: %w", err)
		}
		log.Info("Created Deployment", "name", deploymentName)
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeNormal, "DeploymentCreated", "Created Deployment %s", deploymentName)
			log.V(1).Info("Emitted Kubernetes event", "reason", "DeploymentCreated", "object", deploymentName)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get Deployment: %w", err)
	}

	// Update existing Deployment with retry on conflict (no extra rollout: we only Update when spec differs)
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			if err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: req.Namespace}, existing); err != nil {
				return fmt.Errorf("failed to get Deployment: %w", err)
			}
		}
		if equality.Semantic.DeepEqual(existing.Spec, deployment.Spec) {
			return nil
		}
		existing.Spec = deployment.Spec
		if err := r.Update(ctx, existing); err != nil {
			if apierrors.IsConflict(err) && attempt < maxRetries-1 {
				log.Info("Deployment update conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries, "name", deploymentName)
				baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
				jitter := time.Duration(rand.Intn(50)) * time.Millisecond
				time.Sleep(baseDelay + jitter)
				continue
			}
			return fmt.Errorf("failed to update Deployment: %w", err)
		}
		log.Info("Updated Deployment", "name", deploymentName)
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeNormal, "DeploymentUpdated", "Updated Deployment %s", deploymentName)
			log.V(1).Info("Emitted Kubernetes event", "reason", "DeploymentUpdated", "object", deploymentName)
		}
		return nil
	}
	return fmt.Errorf("failed to update Deployment after %d attempts", maxRetries)
}

// getResourceRequirements returns resource requirements from spec or default values.
func (r *DataFlowReconciler) getResourceRequirements(dataflow *dataflowv1.DataFlow) corev1.ResourceRequirements {
	// If resources specified in spec, use them
	if dataflow.Spec.Resources != nil {
		return *dataflow.Spec.Resources
	}

	// Otherwise use default values
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}

// cleanupResources deletes Deployment, ConfigMaps, and processor RBAC.
func (r *DataFlowReconciler) cleanupResources(ctx context.Context, req ctrl.Request) error {
	log := log.FromContext(ctx)

	deploymentName := k8snames.ProcessorDeployment(req.Name)
	configMapName := k8snames.ProcessorSpecConfigMap(req.Name)
	checkpointConfigMapName := k8snames.ProcessorCheckpointConfigMap(req.Name)
	saName := k8snames.ProcessorServiceAccount(req.Name)

	// Delete Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, deployment); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete Deployment", "name", deploymentName)
		return err
	}
	log.Info("Deleted Deployment", "name", deploymentName)

	// Delete spec ConfigMap
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, configMap); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete ConfigMap", "name", configMapName)
		return err
	}
	log.Info("Deleted ConfigMap", "name", configMapName)

	// Delete checkpoint ConfigMap (if checkpoint persistence was enabled)
	checkpointCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      checkpointConfigMapName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, checkpointCM); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete checkpoint ConfigMap", "name", checkpointConfigMapName)
		return err
	}
	log.Info("Deleted checkpoint ConfigMap", "name", checkpointConfigMapName)

	// Delete processor RBAC (RoleBinding, Role, ServiceAccount)
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, roleBinding); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete RoleBinding", "name", saName)
		return err
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete Role", "name", saName)
		return err
	}
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: req.Namespace,
		},
	}
	if err := r.Delete(ctx, sa); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "failed to delete ServiceAccount", "name", saName)
		return err
	}
	log.Info("Deleted processor RBAC", "name", saName)

	return nil
}
