package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

func TestDataFlowCronReconcile_CreatesConfigMapAndCronJob(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = dataflowv1.AddToScheme(scheme)
	_ = clientgoscheme.AddToScheme(scheme)

	dfc := &dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "default"},
		Spec: dataflowv1.DataFlowCronSpec{
			Schedule: "*/5 * * * *",
			DataFlowSpec: dataflowv1.DataFlowSpec{
				Source: dataflowv1.SourceSpec{Type: "kafka"},
				Sink:   dataflowv1.SinkSpec{Type: "kafka"},
			},
			Triggers: []dataflowv1.DataFlowCronTrigger{{
				Image: "curlimages/curl:8.8.0",
			}},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlowCron{}).
		WithObjects(dfc).
		Build()
	r := NewDataFlowCronReconciler(c, scheme)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cron", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{Name: k8snames.CronSpecConfigMap("cron"), Namespace: "default"}, &cm); err != nil {
		t.Fatalf("configmap not created: %v", err)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Name: k8snames.CronJobName("cron"), Namespace: "default"}, &cj); err != nil {
		t.Fatalf("cronjob not created: %v", err)
	}
	got := cj.Spec.JobTemplate.Spec.Template.Labels[dataFlowCronTriggerIndexLabel]
	if got != dataFlowCronProcessorStepLabel {
		t.Fatalf("processor trigger-index label = %q, want %q", got, dataFlowCronProcessorStepLabel)
	}
}

func TestTriggerIndexLabelAndParse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		idx  int
		want string
	}{
		{-1, dataFlowCronProcessorStepLabel},
		{0, "0"},
		{3, "3"},
	}
	for _, tc := range cases {
		label := triggerIndexLabel(tc.idx)
		if label != tc.want {
			t.Fatalf("triggerIndexLabel(%d) = %q, want %q", tc.idx, label, tc.want)
		}
		if parseTriggerIndex(label) != tc.idx {
			t.Fatalf("parseTriggerIndex(%q) = %d, want %d", label, parseTriggerIndex(label), tc.idx)
		}
	}
	if parseTriggerIndex("invalid") != -2 {
		t.Fatalf("parseTriggerIndex(invalid) = %d, want -2", parseTriggerIndex("invalid"))
	}
}

func TestDataFlowCronReconcile_CreatesFirstTriggerJobAfterProcessor(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = dataflowv1.AddToScheme(scheme)
	_ = clientgoscheme.AddToScheme(scheme)

	dfc := &dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "cron2", Namespace: "default"},
		Spec: dataflowv1.DataFlowCronSpec{
			Schedule: "*/5 * * * *",
			DataFlowSpec: dataflowv1.DataFlowSpec{
				Source: dataflowv1.SourceSpec{Type: "kafka"},
				Sink:   dataflowv1.SinkSpec{Type: "kafka"},
			},
			Triggers: []dataflowv1.DataFlowCronTrigger{
				{Image: "curlimages/curl:8.8.0"},
				{Image: "bitnami/kubectl:latest"},
			},
		},
	}
	done := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron2-processor",
			Namespace: "default",
			Labels: map[string]string{
				dataFlowCronOwnerLabel:        "cron2",
				dataFlowCronTriggerIndexLabel: dataFlowCronProcessorStepLabel,
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
			Ready: &[]int32{1}[0],
			CompletionTime: func() *metav1.Time {
				if !done {
					return nil
				}
				now := metav1.Now()
				return &now
			}(),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlowCron{}).
		WithObjects(dfc, job).
		Build()
	r := NewDataFlowCronReconciler(c, scheme)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cron2", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs); err != nil {
		t.Fatalf("list jobs failed: %v", err)
	}
	if len(jobs.Items) < 2 {
		t.Fatalf("expected first trigger job to be created after processor")
	}
}
