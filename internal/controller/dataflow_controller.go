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
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/operator/runtimeimage"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

// DataFlowFinalizer is the finalizer name for DataFlow. While present, the object
// is not removed from etcd; the controller runs cleanup (Deployment, ConfigMap) and
// then removes this finalizer so deletion can complete.
const DataFlowFinalizer = "dataflow.dataflow.io/finalizer"

// dataflowRefForEvent returns a minimal DataFlow object for use as involvedObject in events
// when the full object is unavailable (e.g. FailedGet). Implements runtime.Object for EventRecorder.
func dataflowRefForEvent(namespace, name string) *dataflowv1.DataFlow {
	return &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: dataflowv1.GroupVersion.String(),
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
}

// DataFlowReconciler reconciles a DataFlow object
type DataFlowReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Recorder       record.EventRecorder
	secretResolver *SecretResolver
	processorImage string
	// operatorDeploymentName/Namespace — for watching the operator Deployment; when it updates we reconcile all DataFlows.
	operatorDeploymentName      string
	operatorDeploymentNamespace string
	watchSecrets                bool
}

func NewDataFlowReconciler(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder) *DataFlowReconciler {
	if recorder == nil {
		ctrl.Log.WithName("dataflow-controller").Info("EventRecorder is nil, Kubernetes events will not be emitted")
	}
	processorImage := runtimeimage.ProcessorImage()

	return &DataFlowReconciler{
		Client:                      client,
		Scheme:                      scheme,
		Recorder:                    recorder,
		secretResolver:              NewSecretResolver(client),
		processorImage:              processorImage,
		operatorDeploymentName:      os.Getenv("OPERATOR_DEPLOYMENT_NAME"),
		operatorDeploymentNamespace: os.Getenv("OPERATOR_NAMESPACE"),
		watchSecrets:                os.Getenv("WATCH_SECRETS") != "false",
	}
}

// ensureDataFlowFinalizer adds DataFlowFinalizer to the DataFlow if not present.
// Call before creating the first child resource (ConfigMap or Deployment).
func (r *DataFlowReconciler) ensureDataFlowFinalizer(ctx context.Context, req ctrl.Request) error {
	var df dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
		return err
	}
	for _, f := range df.Finalizers {
		if f == DataFlowFinalizer {
			return nil
		}
	}
	df.Finalizers = append(df.Finalizers, DataFlowFinalizer)
	return r.Update(ctx, &df)
}

// removeDataFlowFinalizer removes DataFlowFinalizer from the DataFlow so the object can be deleted.
func (r *DataFlowReconciler) removeDataFlowFinalizer(ctx context.Context, req ctrl.Request) error {
	var df dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
		return err
	}
	var newFinalizers []string
	for _, f := range df.Finalizers {
		if f != DataFlowFinalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}
	if len(newFinalizers) == len(df.Finalizers) {
		return nil
	}
	df.Finalizers = newFinalizers
	return r.Update(ctx, &df)
}

// reconcileTimeout returns the timeout for a single Reconcile run from env RECONCILE_TIMEOUT_SECONDS (default 180s).
func reconcileTimeout() time.Duration {
	const defaultSeconds = 180
	s := os.Getenv("RECONCILE_TIMEOUT_SECONDS")
	if s == "" {
		return defaultSeconds * time.Second
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return defaultSeconds * time.Second
	}
	return time.Duration(sec) * time.Second
}

// pendingRequeueAfter returns requeue interval for waiting phases from env RECONCILE_PENDING_REQUEUE_SECONDS (default 20s).
func pendingRequeueAfter() time.Duration {
	const defaultSeconds = 20
	s := os.Getenv("RECONCILE_PENDING_REQUEUE_SECONDS")
	if s == "" {
		return defaultSeconds * time.Second
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return defaultSeconds * time.Second
	}
	return time.Duration(sec) * time.Second
}

// maxConcurrentReconciles returns controller concurrency from env MAX_CONCURRENT_RECONCILES (default 1).
func maxConcurrentReconciles() int {
	const defaultValue = 1
	s := os.Getenv("MAX_CONCURRENT_RECONCILES")
	if s == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return defaultValue
	}
	return n
}

func shouldRequeueAfterPhase(phase string) bool {
	return phase == "Pending"
}

// genReconcileID returns a short hex string for correlating logs within one reconcile.
func genReconcileID() string {
	b := make([]byte, 4)
	if _, err := crand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:8]
	}
	return hex.EncodeToString(b)
}

const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// restoreSpecFromLastApplied restores source/sink config from last-applied-configuration
// when the API stripped it (e.g. due to CRD schema pruning). Modifies dataflow in place.
func restoreSpecFromLastApplied(dataflow *dataflowv1.DataFlow) error {
	raw, ok := dataflow.Annotations[lastAppliedAnnotation]
	if !ok || raw == "" {
		return nil
	}
	var applied struct {
		Spec struct {
			Source struct {
				Config *runtime.RawExtension `json:"config,omitempty"`
			} `json:"source"`
			Sink struct {
				Config *runtime.RawExtension `json:"config,omitempty"`
			} `json:"sink"`
			Errors *struct {
				Config *runtime.RawExtension `json:"config,omitempty"`
			} `json:"errors,omitempty"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(raw), &applied); err != nil {
		return err
	}
	if (dataflow.Spec.Source.Config == nil || len(dataflow.Spec.Source.Config.Raw) == 0) &&
		applied.Spec.Source.Config != nil && len(applied.Spec.Source.Config.Raw) > 0 {
		dataflow.Spec.Source.Config = applied.Spec.Source.Config
	}
	if (dataflow.Spec.Sink.Config == nil || len(dataflow.Spec.Sink.Config.Raw) == 0) &&
		applied.Spec.Sink.Config != nil && len(applied.Spec.Sink.Config.Raw) > 0 {
		dataflow.Spec.Sink.Config = applied.Spec.Sink.Config
	}
	if dataflow.Spec.Errors != nil && applied.Spec.Errors != nil &&
		(dataflow.Spec.Errors.Config == nil || len(dataflow.Spec.Errors.Config.Raw) == 0) &&
		applied.Spec.Errors.Config != nil && len(applied.Spec.Errors.Config.Raw) > 0 {
		dataflow.Spec.Errors.Config = applied.Spec.Errors.Config
	}
	return nil
}

//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DataFlowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	metrics.ControllerReconcileInflight.Inc()
	defer func() {
		metrics.ControllerReconcileInflight.Dec()
	}()

	reconcileID := genReconcileID()
	reconcileLogger := log.FromContext(ctx).WithValues(
		logkeys.DataflowName, req.Name,
		logkeys.DataflowNamespace, req.Namespace,
		logkeys.ReconcileID, reconcileID,
	)
	ctx = log.IntoContext(ctx, reconcileLogger)
	log := log.FromContext(ctx)

	timeout := reconcileTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dataflow dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &dataflow); err != nil {
		metrics.RecordControllerReconcileError("get_dataflow")
		metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
		log.Error(err, "unable to fetch DataFlow")
		if r.Recorder != nil && !apierrors.IsNotFound(err) {
			// Use minimal object reference for event (we don't have UID/ResourceVersion since Get failed)
			ref := dataflowRefForEvent(req.Namespace, req.Name)
			r.Recorder.Event(ref, corev1.EventTypeWarning, "FailedGet", "Unable to fetch DataFlow")
			log.V(1).Info("Emitted Kubernetes event", "reason", "FailedGet", "object", req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling DataFlow")

	// Restore config from last-applied-configuration if API stripped it (CRD pruning).
	if err := restoreSpecFromLastApplied(&dataflow); err != nil {
		log.V(1).Info("Could not restore spec from last-applied-configuration", "error", err)
	}

	if !dataflow.DeletionTimestamp.IsZero() {
		res, err := r.handleDeletion(ctx, req, &dataflow)
		result := "success"
		if err != nil {
			result = "error"
			metrics.RecordControllerReconcileError("handle_deletion")
		}
		metrics.ObserveControllerReconcileDuration(result, time.Since(start).Seconds())
		return res, err
	}

	if err := r.reconcileResources(ctx, req, &dataflow); err != nil {
		metrics.RecordControllerReconcileError("reconcile_resources")
		metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
		return ctrl.Result{}, err
	}

	// Check Deployment status
	deployment := &appsv1.Deployment{}
	deploymentName := k8snames.ProcessorDeployment(dataflow.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: req.Namespace}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			dataflow.Status.Phase = "Pending"
			dataflow.Status.Message = "Deployment not found"
			dataflow.Status.Conditions = buildStatusConditions(dataflow.Status.Phase, dataflow.Status.Message, true, false, "DeploymentNotFound")
		} else {
			metrics.RecordControllerReconcileError("get_deployment")
			metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
			log.Error(err, "failed to get Deployment")
			return ctrl.Result{}, err
		}
	} else {
		desired := desiredProcessorReplicas(&dataflow.Spec)
		ready := deployment.Status.ReadyReplicas
		// Update status based on Deployment state
		if desired == 0 {
			dataflow.Status.Phase = "Pending"
			dataflow.Status.Message = "Processor scaled to zero replicas"
			dataflow.Status.Conditions = buildStatusConditions(dataflow.Status.Phase, dataflow.Status.Message, true, false, "DeploymentScaledToZero")
		} else if ready >= desired {
			dataflow.Status.Phase = "Running"
			if desired == 1 {
				dataflow.Status.Message = "Processor pod is running"
			} else {
				dataflow.Status.Message = fmt.Sprintf("%d/%d processor replicas ready", ready, desired)
			}
			dataflow.Status.Conditions = buildStatusConditions(dataflow.Status.Phase, dataflow.Status.Message, true, true, "DeploymentReady")
		} else if deployment.Status.Replicas > 0 || ready > 0 {
			dataflow.Status.Phase = "Pending"
			if desired == 1 {
				dataflow.Status.Message = "Processor pod is starting"
			} else {
				dataflow.Status.Message = fmt.Sprintf("%d/%d processor replicas ready", ready, desired)
			}
			dataflow.Status.Conditions = buildStatusConditions(dataflow.Status.Phase, dataflow.Status.Message, true, false, "DeploymentStarting")
		} else {
			dataflow.Status.Phase = "Error"
			dataflow.Status.Message = "No replicas available"
			dataflow.Status.Conditions = buildStatusConditions(dataflow.Status.Phase, dataflow.Status.Message, true, false, "DeploymentUnavailable")
		}
	}

	// Update metrics with current status
	metrics.SetDataFlowStatus(req.Namespace, req.Name, dataflow.Status.Phase)

	// Update status with retry logic to handle optimistic locking conflicts
	// Use reconcile context so fake client and real API find the object in the same context.
	// Save status values before update.
	statusPhase := dataflow.Status.Phase
	statusMessage := dataflow.Status.Message
	statusProcessedCount := dataflow.Status.ProcessedCount
	statusErrorCount := dataflow.Status.ErrorCount
	statusLastProcessedTime := dataflow.Status.LastProcessedTime
	statusConditions := dataflow.Status.Conditions

	if err := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = statusPhase
		df.Status.Message = statusMessage
		df.Status.ProcessedCount = statusProcessedCount
		df.Status.ErrorCount = statusErrorCount
		df.Status.LastProcessedTime = statusLastProcessedTime
		df.Status.Conditions = statusConditions
	}); err != nil {
		metrics.RecordControllerReconcileError("update_status")
		log.Error(err, "unable to update DataFlow status")
		if r.Recorder != nil {
			r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "StatusUpdateFailed", "Unable to update DataFlow status: %v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "StatusUpdateFailed", "object", req.NamespacedName)
		}
		// Don't return error if context was canceled or timed out, just requeue
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
			return ctrl.Result{Requeue: true}, nil
		}
		// Object may have been deleted between reconcile start and status update — don't return error
		if apierrors.IsNotFound(err) {
			metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
			return ctrl.Result{}, nil
		}
		// If conflict, schedule retry
		if apierrors.IsConflict(err) {
			metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
			return ctrl.Result{Requeue: true}, nil
		}
		metrics.ObserveControllerReconcileDuration("error", time.Since(start).Seconds())
		return ctrl.Result{}, err
	}

	if shouldRequeueAfterPhase(statusPhase) {
		metrics.ObserveControllerReconcileDuration("success", time.Since(start).Seconds())
		result := ctrl.Result{RequeueAfter: pendingRequeueAfter()}
		log.Info("Reconcile completed",
			logkeys.Phase, statusPhase,
			logkeys.DurationMS, time.Since(start).Milliseconds(),
			logkeys.ProcessedCount, statusProcessedCount,
			logkeys.ErrorCount, statusErrorCount,
			"requeue_after", result.RequeueAfter.String(),
		)
		return result, nil
	}

	metrics.ObserveControllerReconcileDuration("success", time.Since(start).Seconds())
	log.Info("Reconcile completed",
		logkeys.Phase, statusPhase,
		logkeys.DurationMS, time.Since(start).Milliseconds(),
		logkeys.ProcessedCount, statusProcessedCount,
		logkeys.ErrorCount, statusErrorCount,
	)
	return ctrl.Result{}, nil
}

// handleDeletion runs cleanup when DataFlow is being deleted and our finalizer is present.
func (r *DataFlowReconciler) handleDeletion(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	hasOurFinalizer := false
	for _, f := range dataflow.Finalizers {
		if f == DataFlowFinalizer {
			hasOurFinalizer = true
			break
		}
	}
	if !hasOurFinalizer {
		return ctrl.Result{}, nil
	}

	if err := r.cleanupResources(ctx, req); err != nil {
		log.Error(err, "failed to cleanup resources")
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "CleanupFailed", "Failed to cleanup resources: %v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "CleanupFailed", "object", req.NamespacedName)
		}
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(dataflow, corev1.EventTypeNormal, "ResourcesDeleted", "Deleted Deployment and ConfigMap")
		log.V(1).Info("Emitted Kubernetes event", "reason", "ResourcesDeleted", "object", req.NamespacedName)
	}

	if err := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = "Stopped"
		df.Status.Message = "DataFlow resources are cleaned up"
		df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, true, false, "DeploymentDeleted")
	}); err != nil {
		log.Error(err, "unable to update DataFlow status")
	}
	metrics.SetDataFlowStatus(req.Namespace, req.Name, "Stopped")

	if err := r.removeDataFlowFinalizer(ctx, req); err != nil {
		log.Error(err, "unable to remove DataFlow finalizer")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileResources resolves secrets and creates/updates ConfigMap, RBAC, and Deployment.
func (r *DataFlowReconciler) reconcileResources(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow) error {
	log := log.FromContext(ctx)

	resolvedSpec, err := r.secretResolver.ResolveDataFlowSpec(ctx, req.Namespace, &dataflow.Spec)
	if err != nil {
		log.Error(err, "failed to resolve secrets")
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "SecretResolutionFailed", "Failed to resolve secrets: %v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "SecretResolutionFailed", "object", req.NamespacedName)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to resolve secrets: %v", err)
			df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, false, false, "Unknown")
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return err
	}

	resetRequested := applyCheckpointResetIntent(dataflow, resolvedSpec)

	if err := validateNessieSinkObjectStorageRefs(&resolvedSpec.Sink); err != nil {
		log.Error(err, "invalid Nessie sink object storage configuration")
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "InvalidSpec", "%v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "InvalidSpec", "object", req.NamespacedName)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = err.Error()
			df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, false, false, "Unknown")
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return err
	}

	if err := r.createOrUpdateConfigMap(ctx, req, resolvedSpec); err != nil {
		log.Error(err, "failed to create or update ConfigMap")
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "ConfigMapFailed", "Failed to create or update ConfigMap: %v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "ConfigMapFailed", "object", req.NamespacedName)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to create ConfigMap: %v", err)
			df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, true, false, "Unknown")
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return err
	}

	checkpointOn := resolvedSpec.CheckpointPersistence == nil || *resolvedSpec.CheckpointPersistence
	nessieLocalS3Secrets := catalogSinkUsesLocalObjectStorageSecretRefs(&resolvedSpec.Sink, req.Namespace)

	if checkpointOn {
		if err := r.createOrUpdateCheckpointConfigMap(ctx, req, dataflow); err != nil {
			log.Error(err, "failed to create or update checkpoint ConfigMap")
			if r.Recorder != nil {
				r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "CheckpointConfigMapFailed", "Failed to create checkpoint ConfigMap: %v", err)
			}
			updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
				df.Status.Phase = "Error"
				df.Status.Message = fmt.Sprintf("Failed to create checkpoint ConfigMap: %v", err)
				df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, true, false, "Unknown")
			})
			if updateErr != nil {
				log.Error(updateErr, "unable to update DataFlow status")
			}
			return err
		}
	}
	if checkpointOn || nessieLocalS3Secrets {
		if err := r.createOrUpdateProcessorRBAC(ctx, req, dataflow, resolvedSpec); err != nil {
			log.Error(err, "failed to create or update processor RBAC")
			if r.Recorder != nil {
				r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "ProcessorRBACFailed", "Failed to create processor RBAC: %v", err)
			}
			updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
				df.Status.Phase = "Error"
				df.Status.Message = fmt.Sprintf("Failed to create processor RBAC: %v", err)
				df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, true, false, "Unknown")
			})
			if updateErr != nil {
				log.Error(updateErr, "unable to update DataFlow status")
			}
			return err
		}
	}

	if err := r.createOrUpdateDeployment(ctx, req, dataflow, resolvedSpec); err != nil {
		log.Error(err, "failed to create or update Deployment")
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeWarning, "DeploymentFailed", "Failed to create or update Deployment: %v", err)
			log.V(1).Info("Emitted Kubernetes event", "reason", "DeploymentFailed", "object", req.NamespacedName)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to create Deployment: %v", err)
			df.Status.Conditions = buildStatusConditions(df.Status.Phase, df.Status.Message, true, false, "Unknown")
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return err
	}

	if resetRequested {
		if err := r.consumeCheckpointResetFlags(ctx, req.NamespacedName); err != nil {
			log.Error(err, "failed to clear checkpoint reset flags")
			return err
		}
	}

	return nil
}
