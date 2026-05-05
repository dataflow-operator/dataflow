package v1

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func rawConfig(v any) *runtime.RawExtension {
	b, _ := json.Marshal(v)
	return &runtime.RawExtension{Raw: b}
}

func TestValidateDataFlowCronSpec(t *testing.T) {
	valid := &DataFlowCronSpec{
		Schedule: "*/5 * * * *",
		DataFlowSpec: DataFlowSpec{
			Source: SourceSpec{Type: "kafka", Config: rawConfig(map[string]any{
				"brokers": []string{"kafka:9092"},
				"topic":   "input",
			})},
			Sink: SinkSpec{Type: "kafka", Config: rawConfig(map[string]any{
				"brokers": []string{"kafka:9092"},
				"topic":   "output",
			})},
		},
		Triggers: []DataFlowCronTrigger{{Image: "curlimages/curl:8.8.0"}},
	}
	if errs := ValidateDataFlowCronSpec(valid); len(errs) != 0 {
		t.Fatalf("expected no validation errors, got: %v", errs)
	}
}

func TestValidateDataFlowCronSpec_Invalid(t *testing.T) {
	spec := &DataFlowCronSpec{
		Schedule: "bad",
		DataFlowSpec: DataFlowSpec{
			Source: SourceSpec{},
			Sink:   SinkSpec{},
		},
		Triggers: []DataFlowCronTrigger{{Image: ""}},
	}
	errs := ValidateDataFlowCronSpec(spec)
	if len(errs) == 0 {
		t.Fatalf("expected validation errors")
	}
	requireField(t, errs, "spec.schedule")
	requireField(t, errs, "spec.triggers[0].image")
}

func TestDataFlowCronWebhookValidateCreate(t *testing.T) {
	obj := &DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "default"},
		Spec: DataFlowCronSpec{
			Schedule: "*/5 * * * *",
			DataFlowSpec: DataFlowSpec{
				Source: SourceSpec{Type: "kafka", Config: rawConfig(map[string]any{
					"brokers": []string{"kafka:9092"},
					"topic":   "input",
				})},
				Sink: SinkSpec{Type: "kafka", Config: rawConfig(map[string]any{
					"brokers": []string{"kafka:9092"},
					"topic":   "output",
				})},
			},
		},
	}
	_, err := obj.ValidateCreate(context.Background(), runtime.Object(obj))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func requireField(t *testing.T, errs field.ErrorList, path string) {
	t.Helper()
	for _, err := range errs {
		if err.Field == path {
			return
		}
	}
	t.Fatalf("expected error field %q, got %v", path, errs)
}
