package providers

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestRegisterSource_MergesMetadata(t *testing.T) {
	originalSources := sources
	originalSinks := sinks
	sources = map[string]SourceDefinition{}
	sinks = map[string]SinkDefinition{}
	t.Cleanup(func() {
		sources = originalSources
		sinks = originalSinks
	})

	RegisterSource(SourceDefinition{Type: "custom"})
	if SourceSupportsCheckpoint("custom") {
		t.Fatalf("expected checkpoint support to be disabled by default")
	}

	validatorCalled := false
	RegisterSource(SourceDefinition{
		Type:               "custom",
		SupportsCheckpoint: true,
		ValidateConfig: func(raw []byte, _ *field.Path) field.ErrorList {
			validatorCalled = true
			return nil
		},
	})

	if !SourceSupportsCheckpoint("custom") {
		t.Fatalf("expected checkpoint support to be enabled after merge")
	}
	if len(ListSourceTypes()) != 1 || ListSourceTypes()[0] != "custom" {
		t.Fatalf("expected custom source type to be listed")
	}
	validator := SourceValidator("custom")
	if validator == nil {
		t.Fatalf("expected source validator to be registered")
	}
	validator(nil, field.NewPath("spec"))
	if !validatorCalled {
		t.Fatalf("expected merged validator to be callable")
	}
}

func TestRegisterSink_ListsTypesAndValidator(t *testing.T) {
	originalSources := sources
	originalSinks := sinks
	sources = map[string]SourceDefinition{}
	sinks = map[string]SinkDefinition{}
	t.Cleanup(func() {
		sources = originalSources
		sinks = originalSinks
	})

	RegisterSink(SinkDefinition{Type: "sink-b"})
	RegisterSink(SinkDefinition{
		Type: "sink-a",
		ValidateConfig: func(raw []byte, _ *field.Path) field.ErrorList {
			return nil
		},
	})

	types := ListSinkTypes()
	if len(types) != 2 || types[0] != "sink-a" || types[1] != "sink-b" {
		t.Fatalf("expected sorted sink type list, got %v", types)
	}
	if SinkValidator("sink-a") == nil {
		t.Fatalf("expected sink validator to be registered")
	}
}
