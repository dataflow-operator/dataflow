package providers

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func resetRegistries(t *testing.T) {
	t.Helper()
	originalSources := sources
	originalSinks := sinks
	sources = map[string]SourceDefinition{}
	sinks = map[string]SinkDefinition{}
	t.Cleanup(func() {
		sources = originalSources
		sinks = originalSinks
	})
}

func noopSourceValidator([]byte, *field.Path) field.ErrorList { return nil }

func TestRegisterSource_StoresMetadata(t *testing.T) {
	resetRegistries(t)

	validatorCalled := false
	RegisterSource(SourceDefinition{
		Type:               "custom",
		SupportsCheckpoint: true,
		Exhausts:           true,
		ValidateConfig: func(raw []byte, _ *field.Path) field.ErrorList {
			validatorCalled = true
			return nil
		},
	})

	if !SourceSupportsCheckpoint("custom") {
		t.Fatal("expected checkpoint support to be enabled")
	}
	if !SourceExhausts("custom") {
		t.Fatal("expected source to exhaust")
	}
	if SourceExhausts("missing") {
		t.Fatal("unknown source must not exhaust")
	}
	if len(ListSourceTypes()) != 1 || ListSourceTypes()[0] != "custom" {
		t.Fatalf("expected custom source type to be listed")
	}
	validator := SourceValidator("custom")
	if validator == nil {
		t.Fatal("expected source validator to be registered")
	}
	validator(nil, field.NewPath("spec"))
	if !validatorCalled {
		t.Fatal("expected validator to be callable")
	}
}

func TestRegisterSource_RequiresValidator(t *testing.T) {
	resetRegistries(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when validator is missing")
		}
	}()
	RegisterSource(SourceDefinition{Type: "custom", Exhausts: true})
}

func TestRegisterSource_PanicsOnDuplicate(t *testing.T) {
	resetRegistries(t)
	RegisterSource(SourceDefinition{Type: "custom", ValidateConfig: noopSourceValidator})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate source registration")
		}
	}()
	RegisterSource(SourceDefinition{
		Type:               "custom",
		SupportsCheckpoint: true,
		ValidateConfig:     noopSourceValidator,
	})
}

func TestRegisterSink_ListsTypesAndValidator(t *testing.T) {
	resetRegistries(t)

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
		t.Fatal("expected sink validator to be registered")
	}
}

func TestRegisterSink_PanicsOnDuplicate(t *testing.T) {
	resetRegistries(t)
	RegisterSink(SinkDefinition{Type: "sink-a"})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate sink registration")
		}
	}()
	RegisterSink(SinkDefinition{Type: "sink-a", ValidateConfig: func([]byte, *field.Path) field.ErrorList { return nil }})
}
