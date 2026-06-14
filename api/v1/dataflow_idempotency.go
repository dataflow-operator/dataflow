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
	"github.com/dataflow-operator/dataflow/pkg/providers"
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

func validateIdempotency(spec *DataFlowSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if spec == nil || !isPollingSourceType(spec.Source.Type) {
		return all
	}
	if sinkIsIdempotent(&spec.Sink) {
		return all
	}
	msg := "polling source with non-idempotent main sink may produce duplicates on restart; enable upsertMode on the sink or set strictIdempotency: false to accept this warning"
	if StrictIdempotencyEnabled(spec) {
		all = append(all, field.Invalid(f.Child("sink"), spec.Sink.Type,
			"strictIdempotency is enabled but main sink is not idempotent (enable upsertMode for postgresql/trino/clickhouse sinks)"))
	} else {
		_ = msg // warnings emitted via WarnDataFlowSpec
	}
	return all
}

// WarnDataFlowSpec returns admission warnings for non-fatal spec issues.
func WarnDataFlowSpec(spec *DataFlowSpec) admission.Warnings {
	var warnings admission.Warnings
	if spec == nil {
		return warnings
	}
	if isPollingSourceType(spec.Source.Type) && !sinkIsIdempotent(&spec.Sink) && !StrictIdempotencyEnabled(spec) {
		warnings = append(warnings,
			"polling source with non-idempotent main sink may produce duplicates on restart; enable sink.config.upsertMode or set strictIdempotency: true to reject at admission")
	}
	return warnings
}

func isPollingSourceType(sourceType string) bool {
	return providers.SourceSupportsCheckpoint(sourceType)
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
		return cfg.UpsertMode != nil && *cfg.UpsertMode
	case "clickhouse":
		cfg, err := sink.GetClickHouseConfig()
		if err != nil || cfg == nil {
			return false
		}
		return cfg.UpsertMode != nil && *cfg.UpsertMode
	case "trino":
		cfg, err := sink.GetTrinoConfig()
		if err != nil || cfg == nil {
			return false
		}
		return cfg.UpsertMode != nil && *cfg.UpsertMode
	default:
		return false
	}
}
