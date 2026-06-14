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

package v1

import (
	"fmt"

	"github.com/dataflow-operator/dataflow/pkg/providers"
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func validateErrors(errors *ErrorSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if errors == nil {
		return all
	}
	all = append(all, validateSink(&errors.SinkSpec, f)...)
	if errors.AckPolicy != "" &&
		errors.AckPolicy != ErrorAckPolicyAfterWrite &&
		errors.AckPolicy != ErrorAckPolicyNever &&
		errors.AckPolicy != ErrorAckPolicyAfterMainSinkSuccess {
		all = append(all, field.NotSupported(f.Child("ackPolicy"), errors.AckPolicy, []string{
			ErrorAckPolicyAfterWrite,
			ErrorAckPolicyNever,
			ErrorAckPolicyAfterMainSinkSuccess,
		}))
	}
	return all
}

type outputSinkRef struct {
	sink *SinkSpec
	path *field.Path
	role string
}

func collectOutputSinks(spec *DataFlowSpec, f *field.Path) []outputSinkRef {
	if spec == nil {
		return nil
	}
	refs := []outputSinkRef{{
		sink: &spec.Sink,
		path: f.Child("sink"),
		role: "main sink",
	}}
	if spec.Errors != nil {
		refs = append(refs, outputSinkRef{
			sink: &spec.Errors.SinkSpec,
			path: f.Child("errors"),
			role: "error sink",
		})
	}
	for i, t := range spec.Transformations {
		if t.Type != transformtypes.Router {
			continue
		}
		routerCfg, _ := t.GetRouterConfig()
		if routerCfg == nil {
			continue
		}
		idx := f.Child("transformations").Index(i)
		routesPath := idx.Child("router", "routes")
		if t.Config != nil && len(t.Config.Raw) > 0 {
			routesPath = idx.Child("config", "routes")
		}
		for j, route := range routerCfg.Routes {
			sink := route.Sink
			refs = append(refs, outputSinkRef{
				sink: &sink,
				path: routesPath.Index(j).Child("sink"),
				role: "router route sink",
			})
		}
	}
	return refs
}

func validateIdempotency(spec *DataFlowSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if spec == nil || !isPollingSourceType(spec.Source.Type) {
		return all
	}
	for _, ref := range collectOutputSinks(spec, f) {
		if sinkIsIdempotent(ref.sink) {
			continue
		}
		if StrictIdempotencyEnabled(spec) {
			all = append(all, field.Invalid(ref.path, ref.sink.Type,
				fmt.Sprintf("strictIdempotency is enabled but %s is not idempotent (enable upsertMode and conflictKey for postgresql/trino/clickhouse sinks)", ref.role)))
		}
	}
	return all
}

// WarnDataFlowSpec returns admission warnings for non-fatal spec issues.
func WarnDataFlowSpec(spec *DataFlowSpec) admission.Warnings {
	var warnings admission.Warnings
	if spec == nil {
		return warnings
	}
	if !isPollingSourceType(spec.Source.Type) || StrictIdempotencyEnabled(spec) {
		return warnings
	}
	for _, ref := range collectOutputSinks(spec, field.NewPath("spec")) {
		if sinkIsIdempotent(ref.sink) {
			continue
		}
		warnings = append(warnings,
			fmt.Sprintf("polling source with non-idempotent %s may produce duplicates on restart; enable sink.config.upsertMode and conflictKey or set strictIdempotency: true to reject at admission", ref.role))
	}
	return warnings
}

func isPollingSourceType(sourceType string) bool {
	return providers.SourceSupportsCheckpoint(sourceType)
}

func upsertConfigIsIdempotent(upsertMode *bool, conflictKey *string) bool {
	if upsertMode == nil || !*upsertMode {
		return false
	}
	return resolveConflictKeyForValidation(conflictKey) != ""
}

func sinkIsIdempotent(sink *SinkSpec) bool {
	if sink == nil || sink.Config == nil || len(sink.Config.Raw) == 0 {
		return false
	}
	switch sink.Type {
	case "postgresql":
		cfg, err := sink.GetPostgreSQLConfig()
		if err != nil || cfg == nil {
			return false
		}
		return upsertConfigIsIdempotent(cfg.UpsertMode, cfg.ConflictKey)
	case "clickhouse":
		cfg, err := sink.GetClickHouseConfig()
		if err != nil || cfg == nil {
			return false
		}
		return upsertConfigIsIdempotent(cfg.UpsertMode, cfg.ConflictKey)
	case "trino":
		cfg, err := sink.GetTrinoConfig()
		if err != nil || cfg == nil {
			return false
		}
		return upsertConfigIsIdempotent(cfg.UpsertMode, cfg.ConflictKey)
	default:
		return false
	}
}
