package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/operator/runtimeimage"
	"github.com/dataflow-operator/dataflow/internal/version"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

const (
	DataFlowCronFinalizer           = "dataflow.dataflow.io/cron-finalizer"
	dataFlowCronOwnerLabel          = "dataflow.dataflow.io/dataflow-cron"
	dataFlowCronTriggerIndexLabel   = "dataflow.dataflow.io/trigger-index"
	dataFlowCronProcessorStepLabel  = "processor"
	dataFlowCronRunIDLabel          = "dataflow.dataflow.io/run-id"
	dataFlowCronTemplateGeneratedBy = "dataflow.dataflow.io/generated-by"
)

//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflowcrons,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflowcrons/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflowcrons/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=cronjobs;jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

type DataFlowCronReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	processorImage string
	secretResolver *SecretResolver
}

func NewDataFlowCronReconciler(client client.Client, scheme *runtime.Scheme) *DataFlowCronReconciler {
	return &DataFlowCronReconciler{
		Client:         client,
		Scheme:         scheme,
		processorImage: runtimeimage.ProcessorImage(),
		secretResolver: NewSecretResolver(client),
	}
}

func (r *DataFlowCronReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var dfc dataflowv1.DataFlowCron
	if err := r.Get(ctx, req.NamespacedName, &dfc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !dfc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.handleDeletion(ctx, &dfc)
	}
	if err := r.ensureFinalizer(ctx, &dfc); err != nil {
		return ctrl.Result{}, err
	}
	resolvedSpec, err := r.secretResolver.ResolveDataFlowSpec(ctx, dfc.Namespace, &dfc.Spec.DataFlowSpec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve secrets: %w", err)
	}
	if err := r.reconcileSpecConfigMap(ctx, &dfc, resolvedSpec); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileProcessorManifests(ctx, &dfc, resolvedSpec); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileCronJob(ctx, &dfc, resolvedSpec); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileTriggeredJobs(ctx, &dfc); err != nil {
		return ctrl.Result{}, err
	}
	logger.V(1).Info("reconciled DataFlowCron", "name", req.NamespacedName)
	return ctrl.Result{}, nil
}

func (r *DataFlowCronReconciler) reconcileSpecConfigMap(ctx context.Context, dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) error {
	specJSON, err := json.Marshal(resolvedSpec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	name := k8snames.CronSpecConfigMap(dfc.Name)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: dfc.Namespace},
		Data:       map[string]string{"spec.json": string(specJSON)},
	}
	if err := ctrl.SetControllerReference(dfc, cm, r.Scheme); err != nil {
		return err
	}
	var existing corev1.ConfigMap
	err = r.Get(ctx, types.NamespacedName{Name: name, Namespace: dfc.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	if existing.Data["spec.json"] == cm.Data["spec.json"] {
		return nil
	}
	existing.Data = cm.Data
	return r.Update(ctx, &existing)
}

func (r *DataFlowCronReconciler) reconcileProcessorManifests(ctx context.Context, dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) error {
	checkpointOn := checkpointPersistenceEnabled(resolvedSpec)
	catalogLocalS3Secrets := catalogSinkUsesLocalObjectStorageSecretRefs(&resolvedSpec.Sink, dfc.Namespace)
	if checkpointOn {
		if _, err := createOrUpdateCheckpointConfigMap(ctx, r.Client, r.Scheme, dfc.Namespace, dfc.Name, dfc); err != nil {
			return fmt.Errorf("checkpoint ConfigMap: %w", err)
		}
	}
	if checkpointOn || catalogLocalS3Secrets {
		if err := createOrUpdateProcessorRBAC(ctx, r.Client, r.Scheme, dfc.Namespace, dfc.Name, dfc, resolvedSpec); err != nil {
			return fmt.Errorf("processor RBAC: %w", err)
		}
	}
	return nil
}

func (r *DataFlowCronReconciler) reconcileCronJob(ctx context.Context, dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) error {
	name := k8snames.CronJobName(dfc.Name)
	schedule := dfc.Spec.Schedule
	successHistory := int32(3)
	if dfc.Spec.SuccessfulJobsHistoryLimit != nil {
		successHistory = *dfc.Spec.SuccessfulJobsHistoryLimit
	}
	failedHistory := int32(1)
	if dfc.Spec.FailedJobsHistoryLimit != nil {
		failedHistory = *dfc.Spec.FailedJobsHistoryLimit
	}
	concurrency := batchv1.ForbidConcurrent
	switch dfc.Spec.ConcurrencyPolicy {
	case dataflowv1.DataFlowCronConcurrencyAllow:
		concurrency = batchv1.AllowConcurrent
	case dataflowv1.DataFlowCronConcurrencyReplace:
		concurrency = batchv1.ReplaceConcurrent
	}
	jobTemplate := r.buildFirstStepJobTemplate(dfc, resolvedSpec)
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: dfc.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule,
			ConcurrencyPolicy:          concurrency,
			SuccessfulJobsHistoryLimit: &successHistory,
			FailedJobsHistoryLimit:     &failedHistory,
			StartingDeadlineSeconds:    dfc.Spec.StartingDeadlineSeconds,
			Suspend:                    dfc.Spec.Suspend,
			JobTemplate:                jobTemplate,
		},
	}
	if err := ctrl.SetControllerReference(dfc, cronJob, r.Scheme); err != nil {
		return err
	}
	var existing batchv1.CronJob
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: dfc.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, cronJob)
	}
	if err != nil {
		return err
	}
	existing.Spec = cronJob.Spec
	return r.Update(ctx, &existing)
}

func (r *DataFlowCronReconciler) buildFirstStepJobTemplate(dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) batchv1.JobTemplateSpec {
	labels := map[string]string{
		dataFlowCronOwnerLabel:          dfc.Name,
		dataFlowCronTemplateGeneratedBy: "dataflowcron-controller",
		dataFlowCronTriggerIndexLabel:   dataFlowCronProcessorStepLabel,
	}
	return batchv1.JobTemplateSpec{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       r.processorPodSpec(dfc, resolvedSpec),
			},
		},
	}
}

func (r *DataFlowCronReconciler) reconcileTriggeredJobs(ctx context.Context, dfc *dataflowv1.DataFlowCron) error {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(dfc.Namespace), client.MatchingLabels{dataFlowCronOwnerLabel: dfc.Name}); err != nil {
		return err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if isJobFailed(job) {
			now := metav1.Now()
			dfc.Status.Phase = "Failed"
			dfc.Status.ActiveJobName = job.Name
			dfc.Status.LastFailedTime = &now
			_ = r.Status().Update(ctx, dfc)
			continue
		}
		if !isJobSucceeded(job) {
			continue
		}
		runID := job.Labels[dataFlowCronRunIDLabel]
		if runID == "" {
			runID = shortHash(job.Name)
		}
		if err := r.handleSucceededStep(ctx, dfc, job, runID); err != nil {
			return err
		}
	}
	return nil
}

func (r *DataFlowCronReconciler) handleSucceededStep(ctx context.Context, dfc *dataflowv1.DataFlowCron, job *batchv1.Job, runID string) error {
	idx := parseTriggerIndex(job.Labels[dataFlowCronTriggerIndexLabel])
	nextIndex := idx + 1
	dfc.Status.CurrentRunID = runID
	dfc.Status.ActiveJobName = job.Name
	if idx == -1 && len(dfc.Spec.Triggers) > 0 {
		stepName := stableJobName(dfc.Name, runID, "trigger-0")
		if err := r.ensureStepJob(ctx, dfc, stepName, runID, 0); err != nil {
			return err
		}
		dfc.Status.Phase = "RunningTriggers"
		v := int32(0)
		dfc.Status.CurrentTriggerIndex = &v
		return r.Status().Update(ctx, dfc)
	}
	if idx >= 0 && nextIndex < len(dfc.Spec.Triggers) {
		stepName := stableJobName(dfc.Name, runID, fmt.Sprintf("trigger-%d", nextIndex))
		if err := r.ensureStepJob(ctx, dfc, stepName, runID, nextIndex); err != nil {
			return err
		}
		dfc.Status.Phase = "RunningTriggers"
		v := int32(nextIndex)
		dfc.Status.CurrentTriggerIndex = &v
		return r.Status().Update(ctx, dfc)
	}
	now := metav1.Now()
	dfc.Status.Phase = "Completed"
	dfc.Status.CurrentTriggerIndex = nil
	dfc.Status.LastSuccessfulTime = &now
	return r.Status().Update(ctx, dfc)
}

func (r *DataFlowCronReconciler) ensureStepJob(ctx context.Context, dfc *dataflowv1.DataFlowCron, name, runID string, idx int) error {
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: dfc.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	trigger := dfc.Spec.Triggers[idx]
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dfc.Namespace,
			Labels: map[string]string{
				dataFlowCronOwnerLabel:        dfc.Name,
				dataFlowCronRunIDLabel:        runID,
				dataFlowCronTriggerIndexLabel: triggerIndexLabel(idx),
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					dataFlowCronOwnerLabel:        dfc.Name,
					dataFlowCronRunIDLabel:        runID,
					dataFlowCronTriggerIndexLabel: triggerIndexLabel(idx),
				}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{r.triggerToContainer(trigger, idx)},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(dfc, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *DataFlowCronReconciler) ensureProcessorJob(ctx context.Context, dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec, name, runID string) error {
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: dfc.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dfc.Namespace,
			Labels: map[string]string{
				dataFlowCronOwnerLabel:        dfc.Name,
				dataFlowCronRunIDLabel:        runID,
				dataFlowCronTriggerIndexLabel: dataFlowCronProcessorStepLabel,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					dataFlowCronOwnerLabel:        dfc.Name,
					dataFlowCronRunIDLabel:        runID,
					dataFlowCronTriggerIndexLabel: dataFlowCronProcessorStepLabel,
				}},
				Spec: r.processorPodSpec(dfc, resolvedSpec),
			},
		},
	}
	if err := ctrl.SetControllerReference(dfc, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *DataFlowCronReconciler) processorPodSpec(dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) corev1.PodSpec {
	return corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: processorServiceAccountName(dfc.Name, resolvedSpec, dfc.Namespace),
		Containers: []corev1.Container{
			{
				Name:  "processor",
				Image: r.processorImageFor(dfc),
				Command: []string{
					"/processor",
					"--spec-path=/etc/dataflow/spec.json",
					"--namespace=" + dfc.Namespace,
					"--name=" + dfc.Name,
				},
				Env: []corev1.EnvVar{
					{Name: "LOG_LEVEL", Value: processorLogLevel()},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "spec",
					MountPath: "/etc/dataflow",
					ReadOnly:  true,
				}},
				Resources: resourcesOrDefault(dfc.Spec.Resources),
			},
		},
		Volumes: []corev1.Volume{{
			Name: "spec",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: k8snames.CronSpecConfigMap(dfc.Name)},
				},
			},
		}},
		ImagePullSecrets: dfc.Spec.ImagePullSecrets,
		NodeSelector:     dfc.Spec.NodeSelector,
		Affinity:         dfc.Spec.Affinity,
		Tolerations:      dfc.Spec.Tolerations,
	}
}

func (r *DataFlowCronReconciler) triggerToContainer(trigger dataflowv1.DataFlowCronTrigger, idx int) corev1.Container {
	name := trigger.Name
	if strings.TrimSpace(name) == "" {
		name = fmt.Sprintf("trigger-%d", idx)
	}
	return corev1.Container{
		Name:            name,
		Image:           trigger.Image,
		Command:         trigger.Command,
		Args:            trigger.Args,
		Env:             trigger.Env,
		Resources:       valueOrDefaultResources(trigger.Resources),
		ImagePullPolicy: trigger.ImagePullPolicy,
	}
}

func (r *DataFlowCronReconciler) processorImageFor(dfc *dataflowv1.DataFlowCron) string {
	if img := strings.TrimSpace(dfc.Spec.ProcessorImage); img != "" {
		return img
	}
	if tag := strings.TrimSpace(dfc.Spec.ProcessorVersion); tag != "" {
		return version.ProcessorImageWithTag(tag)
	}
	return r.processorImage
}

func (r *DataFlowCronReconciler) ensureFinalizer(ctx context.Context, dfc *dataflowv1.DataFlowCron) error {
	for _, f := range dfc.Finalizers {
		if f == DataFlowCronFinalizer {
			return nil
		}
	}
	dfc.Finalizers = append(dfc.Finalizers, DataFlowCronFinalizer)
	return r.Update(ctx, dfc)
}

func (r *DataFlowCronReconciler) handleDeletion(ctx context.Context, dfc *dataflowv1.DataFlowCron) error {
	cronName := k8snames.CronJobName(dfc.Name)
	cron := &batchv1.CronJob{}
	if err := r.Get(ctx, types.NamespacedName{Name: cronName, Namespace: dfc.Namespace}, cron); err == nil {
		if err := r.Delete(ctx, cron); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	cmName := k8snames.CronSpecConfigMap(dfc.Name)
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: dfc.Namespace}, cm); err == nil {
		if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if err := deleteProcessorCheckpointAndRBAC(ctx, r.Client, dfc.Namespace, dfc.Name); err != nil {
		return err
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(dfc.Namespace), client.MatchingLabels{dataFlowCronOwnerLabel: dfc.Name}); err == nil {
		for i := range jobs.Items {
			_ = r.Delete(ctx, &jobs.Items[i])
		}
	}
	filtered := make([]string, 0, len(dfc.Finalizers))
	for _, f := range dfc.Finalizers {
		if f != DataFlowCronFinalizer {
			filtered = append(filtered, f)
		}
	}
	dfc.Finalizers = filtered
	return r.Update(ctx, dfc)
}

func (r *DataFlowCronReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dataflowv1.DataFlowCron{}).
		Owns(&batchv1.CronJob{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles()}).
		Complete(r)
}

func isJobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func triggerIndexLabel(idx int) string {
	if idx < 0 {
		return dataFlowCronProcessorStepLabel
	}
	return fmt.Sprintf("%d", idx)
}

func parseTriggerIndex(s string) int {
	if s == dataFlowCronProcessorStepLabel {
		return -1
	}
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return -2
	}
	return i
}

func shortHash(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])[:8]
}

func stableJobName(owner, runID, step string) string {
	base := k8snames.CronRunJobName(owner, runID, step)
	if len(base) <= 63 {
		return base
	}
	hash := shortHash(base)
	cut := 63 - len(hash) - 1
	return base[:cut] + "-" + hash
}

func valueOrDefaultResources(in *corev1.ResourceRequirements) corev1.ResourceRequirements {
	if in == nil {
		return corev1.ResourceRequirements{}
	}
	return *in.DeepCopy()
}

func resourcesOrDefault(in *corev1.ResourceRequirements) corev1.ResourceRequirements {
	if in == nil {
		return corev1.ResourceRequirements{}
	}
	return *in.DeepCopy()
}
