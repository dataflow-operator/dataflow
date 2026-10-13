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
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ConfigValidator checks raw transformation config.
// path is the transformation item, so config fields are reported under path.Child("config").
type ConfigValidator func(raw []byte, path *field.Path) field.ErrorList

// Definition is one transformation type and its admission validator.
type Definition struct {
	Type           string
	ValidateConfig ConfigValidator
}

var (
	registryMu sync.RWMutex
	validators = map[string]ConfigValidator{}
)

// Register adds a transformation validator. Duplicate types and missing validators panic.
func Register(def Definition) {
	if def.Type == "" {
		panic("transformtypes: transformation type is required")
	}
	if def.ValidateConfig == nil {
		panic("transformtypes: transformation validator is required")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := validators[def.Type]; exists {
		panic("transformtypes: duplicate transformation registration: " + def.Type)
	}
	validators[def.Type] = def.ValidateConfig
}

// Validator returns the admission validator for typ, or nil when typ is not registered.
func Validator(typ string) ConfigValidator {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return validators[typ]
}

// RegisteredTypes returns registered transformation types in sorted order.
func RegisteredTypes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	types := make([]string, 0, len(validators))
	for typ := range validators {
		types = append(types, typ)
	}
	sort.Strings(types)
	return types
}
