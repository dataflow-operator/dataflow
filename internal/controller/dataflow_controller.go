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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	crand "crypto/rand"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/version"
)

// DataFlowReconciler reconciles a DataFlow object
type DataFlowReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Recorder       record.EventRecorder
	secretResolver *SecretResolver
	processorImage string
	// operatorDeploymentName/Namespace — для наблюдения за Deployment оператора; при его обновлении реконсилируем все DataFlow.
	operatorDeploymentName      string
	operatorDeploymentNamespace string
}

func NewDataFlowReconciler(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder) *DataFlowReconciler {
	// Образ процессора: из env или тот же образ и версия, что и оператор (задаётся при сборке через ldflags).
	processorImage := version.DefaultProcessorImage()
	if img := os.Getenv("PROCESSOR_IMAGE"); img != "" {
		processorImage = img
	}

	return &DataFlowReconciler{
		Client:                      client,
		Scheme:                      scheme,
		Recorder:                    recorder,
		secretResolver:              NewSecretResolver(client),
		processorImage:              processorImage,
		operatorDeploymentName:      os.Getenv("OPERATOR_DEPLOYMENT_NAME"),
		operatorDeploymentNamespace: os.Getenv("OPERATOR_NAMESPACE"),
	}
}

// updateStatusWithRetry обновляет статус DataFlow с retry логикой для обработки конфликтов оптимистичной блокировки
func (r *DataFlowReconciler) updateStatusWithRetry(ctx context.Context, req ctrl.Request, updateFn func(*dataflowv1.DataFlow)) error {
	log := log.FromContext(ctx)
	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		var df dataflowv1.DataFlow
		if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
			if apierrors.IsNotFound(err) {
				// Если объект не найден, не имеет смысла повторять попытки
				return err
			}
			if attempt < maxRetries-1 {
				log.Error(err, "unable to fetch DataFlow for status update, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
				// Экспоненциальная задержка с jitter: базовая задержка * (2^attempt) + случайная задержка до 50ms
				baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
				jitter := time.Duration(rand.Intn(50)) * time.Millisecond
				time.Sleep(baseDelay + jitter)
				continue
			}
			return fmt.Errorf("failed to fetch DataFlow after %d attempts: %w", maxRetries, err)
		}

		// Применяем функцию обновления статуса
		updateFn(&df)

		if err := r.Status().Update(ctx, &df); err != nil {
			if apierrors.IsConflict(err) {
				if attempt < maxRetries-1 {
					log.Info("status update conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
					// Экспоненциальная задержка с jitter: базовая задержка * (2^attempt) + случайная задержка до 50ms
					baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
					jitter := time.Duration(rand.Intn(50)) * time.Millisecond
					time.Sleep(baseDelay + jitter)
					continue
				}
				// После всех попыток возвращаем конфликт, чтобы вызвать requeue
				return err
			}
			// Для других ошибок возвращаем сразу
			return err
		}

		// Успешное обновление
		if attempt > 0 {
			log.Info("Successfully updated DataFlow status after retry", "attempt", attempt+1)
		}
		return nil
	}

	return fmt.Errorf("failed to update status after %d attempts", maxRetries)
}

// processorLogLevel returns LOG_LEVEL for processor pods (from env PROCESSOR_LOG_LEVEL or default "info").
func processorLogLevel() string {
	if v := os.Getenv("PROCESSOR_LOG_LEVEL"); v != "" {
		return v
	}
	return "info"
}

// genReconcileID returns a short hex string for correlating logs within one reconcile.
func genReconcileID() string {
	b := make([]byte, 4)
	if _, err := crand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:8]
	}
	return hex.EncodeToString(b)
}

//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DataFlowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reconcileID := genReconcileID()
	reconcileLogger := log.FromContext(ctx).WithValues(
		logkeys.DataflowName, req.Name,
		logkeys.DataflowNamespace, req.Namespace,
		logkeys.ReconcileID, reconcileID,
	)
	ctx = log.IntoContext(ctx, reconcileLogger)
	log := log.FromContext(ctx)

	var dataflow dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &dataflow); err != nil {
		log.Error(err, "unable to fetch DataFlow")
		if r.Recorder != nil && !apierrors.IsNotFound(err) {
			ref := &dataflowv1.DataFlow{}
			ref.APIVersion = dataflowv1.GroupVersion.String()
			ref.Kind = "DataFlow"
			ref.Namespace = req.Namespace
			ref.Name = req.Name
			r.Recorder.Event(ref, corev1.EventTypeWarning, "FailedGet", "Unable to fetch DataFlow")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling DataFlow")

	// Check if DataFlow is being deleted
	if !dataflow.DeletionTimestamp.IsZero() {
		// Удаляем Deployment и ConfigMap при удалении DataFlow
		if err := r.cleanupResources(ctx, req); err != nil {
			log.Error(err, "failed to cleanup resources")
			if r.Recorder != nil {
				r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "CleanupFailed", "Failed to cleanup resources: %v", err)
			}
			return ctrl.Result{}, err
		}
		if r.Recorder != nil {
			r.Recorder.Event(&dataflow, corev1.EventTypeNormal, "ResourcesDeleted", "Deleted Deployment and ConfigMap")
		}

		if err := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Stopped"
		}); err != nil {
			log.Error(err, "unable to update DataFlow status")
		}
		metrics.SetDataFlowStatus(req.Namespace, req.Name, "Stopped")
		return ctrl.Result{}, nil
	}

	// Resolve secrets
	resolvedSpec, err := r.secretResolver.ResolveDataFlowSpec(ctx, req.Namespace, &dataflow.Spec)
	if err != nil {
		log.Error(err, "failed to resolve secrets")
		if r.Recorder != nil {
			r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "SecretResolutionFailed", "Failed to resolve secrets: %v", err)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to resolve secrets: %v", err)
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return ctrl.Result{}, err
	}

	// Создаем или обновляем ConfigMap со spec
	if err := r.createOrUpdateConfigMap(ctx, req, resolvedSpec); err != nil {
		log.Error(err, "failed to create or update ConfigMap")
		if r.Recorder != nil {
			r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "ConfigMapFailed", "Failed to create or update ConfigMap: %v", err)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to create ConfigMap: %v", err)
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return ctrl.Result{}, err
	}

	// Создаем или обновляем Deployment
	if err := r.createOrUpdateDeployment(ctx, req, &dataflow); err != nil {
		log.Error(err, "failed to create or update Deployment")
		if r.Recorder != nil {
			r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "DeploymentFailed", "Failed to create or update Deployment: %v", err)
		}
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to create Deployment: %v", err)
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return ctrl.Result{}, err
	}

	// Проверяем статус Deployment
	deployment := &appsv1.Deployment{}
	deploymentName := fmt.Sprintf("dataflow-%s", dataflow.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: req.Namespace}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			dataflow.Status.Phase = "Pending"
			dataflow.Status.Message = "Deployment not found"
		} else {
			log.Error(err, "failed to get Deployment")
			return ctrl.Result{}, err
		}
	} else {
		// Обновляем статус на основе состояния Deployment
		if deployment.Status.ReadyReplicas > 0 {
			dataflow.Status.Phase = "Running"
			dataflow.Status.Message = "Processor pod is running"
		} else if deployment.Status.Replicas > 0 {
			dataflow.Status.Phase = "Pending"
			dataflow.Status.Message = "Processor pod is starting"
		} else {
			dataflow.Status.Phase = "Error"
			dataflow.Status.Message = "No replicas available"
		}
	}

	// Update metrics with current status
	metrics.SetDataFlowStatus(req.Namespace, req.Name, dataflow.Status.Phase)

	// Update status with retry logic to handle optimistic locking conflicts
	// Используем контекст реконсиляции, чтобы fake client и реальный API находили объект по одному контексту
	// Сохраняем значения статуса перед обновлением
	statusPhase := dataflow.Status.Phase
	statusMessage := dataflow.Status.Message
	statusProcessedCount := dataflow.Status.ProcessedCount
	statusErrorCount := dataflow.Status.ErrorCount
	statusLastProcessedTime := dataflow.Status.LastProcessedTime

	if err := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = statusPhase
		df.Status.Message = statusMessage
		df.Status.ProcessedCount = statusProcessedCount
		df.Status.ErrorCount = statusErrorCount
		df.Status.LastProcessedTime = statusLastProcessedTime
	}); err != nil {
		log.Error(err, "unable to update DataFlow status")
		if r.Recorder != nil {
			r.Recorder.Eventf(&dataflow, corev1.EventTypeWarning, "StatusUpdateFailed", "Unable to update DataFlow status: %v", err)
		}
		// Don't return error if context was canceled, just log it
		if err == context.Canceled || err == context.DeadlineExceeded {
			return ctrl.Result{Requeue: true}, nil
		}
		// Объект мог быть удалён между началом реконсиляции и обновлением статуса — не возвращаем ошибку
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		// Если это конфликт, запланируем повторную попытку
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// createOrUpdateConfigMap создает или обновляет ConfigMap со spec
func (r *DataFlowReconciler) createOrUpdateConfigMap(ctx context.Context, req ctrl.Request, spec *dataflowv1.DataFlowSpec) error {
	log := log.FromContext(ctx)

	// Сериализуем spec в JSON
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec: %w", err)
	}

	configMapName := fmt.Sprintf("dataflow-%s-spec", req.Name)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: req.Namespace,
		},
		Data: map[string]string{
			"spec.json": string(specJSON),
		},
	}

	// Получаем DataFlow для установки owner reference
	var df dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
		return fmt.Errorf("failed to get DataFlow: %w", err)
	}

	// Устанавливаем owner reference
	if err := ctrl.SetControllerReference(&df, configMap, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Проверяем, существует ли ConfigMap
	existing := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: req.Namespace}, existing)
	if err != nil && apierrors.IsNotFound(err) {
		// Создаем новый ConfigMap
		if err := r.Create(ctx, configMap); err != nil {
			return fmt.Errorf("failed to create ConfigMap: %w", err)
		}
		log.Info("Created ConfigMap", "name", configMapName)
		if r.Recorder != nil {
			r.Recorder.Eventf(&df, corev1.EventTypeNormal, "ConfigMapCreated", "Created ConfigMap %s", configMapName)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	} else {
		// Обновляем существующий ConfigMap
		existing.Data = configMap.Data
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update ConfigMap: %w", err)
		}
		log.Info("Updated ConfigMap", "name", configMapName)
		if r.Recorder != nil {
			r.Recorder.Eventf(&df, corev1.EventTypeNormal, "ConfigMapUpdated", "Updated ConfigMap %s", configMapName)
		}
	}

	return nil
}

// createOrUpdateDeployment создает или обновляет Deployment для процессора
func (r *DataFlowReconciler) createOrUpdateDeployment(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow) error {
	log := log.FromContext(ctx)

	deploymentName := fmt.Sprintf("dataflow-%s", dataflow.Name)
	configMapName := fmt.Sprintf("dataflow-%s-spec", dataflow.Name)

	labels := map[string]string{
		"app":                        "dataflow-processor",
		"dataflow.dataflow.io/name":  dataflow.Name,
		"dataflow.dataflow.io/owned": "true",
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: req.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: func() *int32 { r := int32(1); return &r }(),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "processor",
							Image: r.processorImage,
							Command: []string{
								"/processor",
								"--spec-path=/etc/dataflow/spec.json",
								"--namespace=" + req.Namespace,
								"--name=" + dataflow.Name,
							},
							Env: []corev1.EnvVar{
								{Name: "LOG_LEVEL", Value: processorLogLevel()},
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
					NodeSelector: dataflow.Spec.NodeSelector,
					Affinity:     dataflow.Spec.Affinity,
					Tolerations:  dataflow.Spec.Tolerations,
				},
			},
		},
	}

	// Устанавливаем owner reference
	if err := ctrl.SetControllerReference(dataflow, deployment, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Проверяем, существует ли Deployment
	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: req.Namespace}, existing)
	if err != nil && apierrors.IsNotFound(err) {
		// Создаем новый Deployment
		if err := r.Create(ctx, deployment); err != nil {
			return fmt.Errorf("failed to create Deployment: %w", err)
		}
		log.Info("Created Deployment", "name", deploymentName)
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeNormal, "DeploymentCreated", "Created Deployment %s", deploymentName)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get Deployment: %w", err)
	} else {
		// Обновляем существующий Deployment только при реальном изменении spec
		if equality.Semantic.DeepEqual(existing.Spec, deployment.Spec) {
			return nil
		}
		existing.Spec = deployment.Spec
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update Deployment: %w", err)
		}
		log.Info("Updated Deployment", "name", deploymentName)
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeNormal, "DeploymentUpdated", "Updated Deployment %s", deploymentName)
		}
	}

	return nil
}

// getResourceRequirements возвращает требования к ресурсам из spec или дефолтные значения
func (r *DataFlowReconciler) getResourceRequirements(dataflow *dataflowv1.DataFlow) corev1.ResourceRequirements {
	// Если ресурсы указаны в spec, используем их
	if dataflow.Spec.Resources != nil {
		return *dataflow.Spec.Resources
	}

	// Иначе используем дефолтные значения
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

// cleanupResources удаляет Deployment и ConfigMap
func (r *DataFlowReconciler) cleanupResources(ctx context.Context, req ctrl.Request) error {
	log := log.FromContext(ctx)

	deploymentName := fmt.Sprintf("dataflow-%s", req.Name)
	configMapName := fmt.Sprintf("dataflow-%s-spec", req.Name)

	// Удаляем Deployment
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

	// Удаляем ConfigMap
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

	return nil
}

// isOperatorDeployment возвращает true, если obj — это Deployment оператора (по имени и namespace).
func (r *DataFlowReconciler) isOperatorDeployment(obj client.Object) bool {
	if r.operatorDeploymentName == "" || r.operatorDeploymentNamespace == "" {
		return false
	}
	return obj.GetNamespace() == r.operatorDeploymentNamespace && obj.GetName() == r.operatorDeploymentName
}

// enqueueAllDataFlowsForOperatorUpdate возвращает запросы на реконсиляцию всех DataFlow (вызывается при обновлении Deployment оператора).
func (r *DataFlowReconciler) enqueueAllDataFlowsForOperatorUpdate(ctx context.Context, o client.Object) []reconcile.Request {
	if !r.isOperatorDeployment(o) {
		return nil
	}
	list := &dataflowv1.DataFlowList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Name: list.Items[i].Name, Namespace: list.Items[i].Namespace,
		}})
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *DataFlowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&dataflowv1.DataFlow{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{})

	// При обновлении Deployment оператора реконсилируем все DataFlow, чтобы поды процессора получили новый образ.
	if r.operatorDeploymentName != "" && r.operatorDeploymentNamespace != "" {
		b = b.Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllDataFlowsForOperatorUpdate),
			builder.WithPredicates(predicate.NewPredicateFuncs(r.isOperatorDeployment)),
		)
	}

	return b.Complete(r)
}
