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

// Package transformtypes holds the canonical list of DataFlow transformation type keys.
// internal/transformers and api/v1 validation both depend on it to avoid drift.
package transformtypes

import (
	"slices"
)

// Canonical transformation type keys (processor registry + API validation).
const (
	Timestamp        = "timestamp"
	Flatten          = "flatten"
	Filter           = "filter"
	Mask             = "mask"
	Router           = "router"
	Select           = "select"
	Remove           = "remove"
	SnakeCase        = "snakeCase"
	CamelCase        = "camelCase"
	DebeziumUnwrap   = "debeziumUnwrap"
	ReplaceField     = "replaceField"
	HeadersToPayload = "headersToPayload"
	StructFlatten    = "structFlatten"
	ExtractField     = "extractField"
	HoistField       = "hoistField"
)

// keys is built from constants only — add a new transformer: new const, append here, register in factory.
var keys = []string{
	Timestamp,
	Flatten,
	Filter,
	Mask,
	Router,
	Select,
	Remove,
	SnakeCase,
	CamelCase,
	DebeziumUnwrap,
	ReplaceField,
	HeadersToPayload,
	StructFlatten,
	ExtractField,
	HoistField,
}

var keySet map[string]struct{}

func init() {
	keySet = make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
}

// All returns a copy of registered transformation type keys in canonical order.
func All() []string {
	return slices.Clone(keys)
}

// IsRegistered reports whether typ is a supported transformation type key.
func IsRegistered(typ string) bool {
	_, ok := keySet[typ]
	return ok
}
