package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFlowCronReconcile_CreatesCheckpointAndRBAC(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = dataflowv1.AddToScheme(scheme)
	_ = clientgoscheme.AddToScheme(scheme)

	trueVal := true
	dfc := &dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "price-exported-migration", Namespace: "dataflow"},
		Spec: dataflowv1.DataFlowCronSpec{
			Schedule: "0 0 1 1 *",
			DataFlowSpec: dataflowv1.DataFlowSpec{
				CheckpointPersistence: &trueVal,
				Source:                dataflowv1.SourceSpec{Type: "postgresql"},
				Sink:                  dataflowv1.SinkSpec{Type: "postgresql"},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlowCron{}).
		WithObjects(dfc).
		Build()
	r := NewDataFlowCronReconciler(c, scheme)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "price-exported-migration", Namespace: "dataflow"},
	})
	require.NoError(t, err)

	var checkpointCM corev1.ConfigMap
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Name: k8snames.ProcessorCheckpointConfigMap("price-exported-migration"), Namespace: "dataflow",
	}, &checkpointCM))

	var sa corev1.ServiceAccount
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Name: k8snames.ProcessorServiceAccount("price-exported-migration"), Namespace: "dataflow",
	}, &sa))

	var role rbacv1.Role
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Name: k8snames.ProcessorServiceAccount("price-exported-migration"), Namespace: "dataflow",
	}, &role))
	require.NotEmpty(t, role.Rules)

	var cj batchv1.CronJob
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Name: k8snames.CronJobName("price-exported-migration"), Namespace: "dataflow",
	}, &cj))
	assert.Equal(t, k8snames.ProcessorServiceAccount("price-exported-migration"),
		cj.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName)
}

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

func TestDataFlowCronReconcile_ResolvesSecretsInConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	const wantDSN = "postgres://user:pass@host:5432/db"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db-env-vars", Namespace: "default"},
		Data: map[string][]byte{
			"pg-source": []byte(wantDSN),
			"pg-sink":   []byte(wantDSN),
		},
	}

	dfc := &dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "cron-secrets", Namespace: "default"},
		Spec: dataflowv1.DataFlowCronSpec{
			Schedule: "0 0 * * *",
			DataFlowSpec: dataflowv1.DataFlowSpec{
				Source: dataflowv1.SourceSpec{
					Type: "postgresql",
					Config: mustConfig(dataflowv1.PostgreSQLSourceSpec{
						Table: "price.price",
						ConnectionStringSecretRef: &dataflowv1.SecretRef{
							Name: "db-env-vars",
							Key:  "pg-source",
						},
					}),
				},
				Sink: dataflowv1.SinkSpec{
					Type: "postgresql",
					Config: mustConfig(dataflowv1.PostgreSQLSinkSpec{
						Table: "public.price_target",
						ConnectionStringSecretRef: &dataflowv1.SecretRef{
							Name: "db-env-vars",
							Key:  "pg-sink",
						},
					}),
				},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlowCron{}).
		WithObjects(secret, dfc).
		Build()
	r := NewDataFlowCronReconciler(c, scheme)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cron-secrets", Namespace: "default"},
	})
	require.NoError(t, err)

	var cm corev1.ConfigMap
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Name: k8snames.CronSpecConfigMap("cron-secrets"), Namespace: "default",
	}, &cm))

	var spec dataflowv1.DataFlowSpec
	require.NoError(t, json.Unmarshal([]byte(cm.Data["spec.json"]), &spec))

	sourceCfg, err := spec.Source.GetPostgreSQLConfig()
	require.NoError(t, err)
	assert.Equal(t, wantDSN, sourceCfg.ConnectionString)

	sinkCfg, err := spec.Sink.GetPostgreSQLConfig()
	require.NoError(t, err)
	assert.Equal(t, wantDSN, sinkCfg.ConnectionString)
}

func TestDataFlowCronReconcile_CheckpointResetOneShot(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	trueVal := true
	dfc := &dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "cron-reset", Namespace: "default"},
		Spec: dataflowv1.DataFlowCronSpec{
			Schedule: "0 0 * * *",
			DataFlowSpec: dataflowv1.DataFlowSpec{
				CheckpointReset:       &trueVal,
				CheckpointPersistence: &trueVal,
				Source:                dataflowv1.SourceSpec{Type: "postgresql"},
				Sink:                  dataflowv1.SinkSpec{Type: "postgresql"},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlowCron{}).
		WithObjects(dfc).
		Build()
	r := NewDataFlowCronReconciler(c, scheme)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "cron-reset", Namespace: "default"}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	var afterFirst dataflowv1.DataFlowCron
	require.NoError(t, c.Get(ctx, req.NamespacedName, &afterFirst))
	require.NotNil(t, afterFirst.Spec.CheckpointReset)
	assert.True(t, *afterFirst.Spec.CheckpointReset, "reset flag must remain until processor job applies it")

	var configMapAfterFirst struct {
		CheckpointReset *bool `json:"checkpointReset"`
	}
	cmName := types.NamespacedName{Name: k8snames.CronSpecConfigMap("cron-reset"), Namespace: "default"}
	var cm corev1.ConfigMap
	require.NoError(t, c.Get(ctx, cmName, &cm))
	require.NotEmpty(t, cm.Annotations[AnnotationCheckpointResetAppliedAt])
	require.NoError(t, json.Unmarshal([]byte(cm.Data["spec.json"]), &configMapAfterFirst))
	require.NotNil(t, configMapAfterFirst.CheckpointReset)
	assert.True(t, *configMapAfterFirst.CheckpointReset)

	appliedAt, err := time.Parse(time.RFC3339Nano, cm.Annotations[AnnotationCheckpointResetAppliedAt])
	require.NoError(t, err)

	oldJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-reset-old",
			Namespace: "default",
			Labels: map[string]string{
				dataFlowCronOwnerLabel:        "cron-reset",
				dataFlowCronTriggerIndexLabel: dataFlowCronProcessorStepLabel,
			},
		},
		Status: batchv1.JobStatus{
			StartTime: &metav1.Time{Time: appliedAt.Add(-time.Hour)},
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	require.NoError(t, c.Create(ctx, oldJob))

	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	var afterOldJob dataflowv1.DataFlowCron
	require.NoError(t, c.Get(ctx, req.NamespacedName, &afterOldJob))
	require.NotNil(t, afterOldJob.Spec.CheckpointReset, "old processor job must not consume reset")

	newJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-reset-new",
			Namespace: "default",
			Labels: map[string]string{
				dataFlowCronOwnerLabel:        "cron-reset",
				dataFlowCronTriggerIndexLabel: dataFlowCronProcessorStepLabel,
			},
		},
		Status: batchv1.JobStatus{
			StartTime: &metav1.Time{Time: appliedAt.Add(time.Minute)},
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	require.NoError(t, c.Create(ctx, newJob))

	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	var afterConsume dataflowv1.DataFlowCron
	require.NoError(t, c.Get(ctx, req.NamespacedName, &afterConsume))
	assert.Nil(t, afterConsume.Spec.CheckpointReset, "reset flag must be consumed after processor job")

	var cmAfterConsume corev1.ConfigMap
	require.NoError(t, c.Get(ctx, cmName, &cmAfterConsume))
	_, hasAppliedAt := cmAfterConsume.Annotations[AnnotationCheckpointResetAppliedAt]
	assert.False(t, hasAppliedAt)

	var configMapAfterConsume struct {
		CheckpointReset *bool `json:"checkpointReset"`
	}
	require.NoError(t, json.Unmarshal([]byte(cmAfterConsume.Data["spec.json"]), &configMapAfterConsume))
	assert.Nil(t, configMapAfterConsume.CheckpointReset)
}
