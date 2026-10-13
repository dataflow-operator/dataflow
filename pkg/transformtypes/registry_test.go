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

package transformtypes

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func resetValidators(t *testing.T) {
	t.Helper()
	registryMu.Lock()
	original := validators
	validators = map[string]ConfigValidator{}
	registryMu.Unlock()
	t.Cleanup(func() {
		registryMu.Lock()
		validators = original
		registryMu.Unlock()
	})
}

func noopValidator([]byte, *field.Path) field.ErrorList { return nil }

func TestRegister_StoresValidator(t *testing.T) {
	resetValidators(t)

	called := false
	Register(Definition{
		Type: "custom",
		ValidateConfig: func(raw []byte, _ *field.Path) field.ErrorList {
			called = true
			return nil
		},
	})

	types := RegisteredTypes()
	if len(types) != 1 || types[0] != "custom" {
		t.Fatalf("expected custom type to be listed, got %v", types)
	}
	validator := Validator("custom")
	if validator == nil {
		t.Fatal("expected validator to be registered")
	}
	validator(nil, field.NewPath("spec"))
	if !called {
		t.Fatal("expected validator to be callable")
	}
	if Validator("missing") != nil {
		t.Fatal("unknown type must not have a validator")
	}
}

func TestRegister_RequiresTypeAndValidator(t *testing.T) {
	resetValidators(t)

	t.Run("empty type", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic when type is empty")
			}
		}()
		Register(Definition{ValidateConfig: noopValidator})
	})

	t.Run("nil validator", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic when validator is missing")
			}
		}()
		Register(Definition{Type: "custom"})
	})
}

func TestRegister_PanicsOnDuplicate(t *testing.T) {
	resetValidators(t)
	Register(Definition{Type: "custom", ValidateConfig: noopValidator})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	Register(Definition{Type: "custom", ValidateConfig: noopValidator})
}

func TestRegisteredTypes_Sorted(t *testing.T) {
	resetValidators(t)
	Register(Definition{Type: "b", ValidateConfig: noopValidator})
	Register(Definition{Type: "a", ValidateConfig: noopValidator})

	types := RegisteredTypes()
	if len(types) != 2 || types[0] != "a" || types[1] != "b" {
		t.Fatalf("expected sorted type list, got %v", types)
	}
}
