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

	"github.com/dataflow-operator/dataflow/pkg/sinkbatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Recommended sink batchSize floors (admission warnings when set below).
// Aliases of pkg/sinkbatch — keep names stable for tests and docs references.
const (
	RecommendedPostgreSQLSinkBatchSize = sinkbatch.RecommendedMinPostgreSQLBatchSize
	RecommendedClickHouseSinkBatchSize = sinkbatch.RecommendedMinClickHouseBatchSize
	RecommendedTrinoSinkBatchSize      = sinkbatch.RecommendedMinTrinoBatchSize
	RecommendedNessieSinkBatchSize     = sinkbatch.RecommendedMinNessieBatchSize
	RecommendedIcebergSinkBatchSize    = sinkbatch.RecommendedMinIcebergBatchSize
)

func warnSinkBatchSizes(spec *DataFlowSpec) admission.Warnings {
	var warnings admission.Warnings
	if spec == nil {
		return warnings
	}
	for _, ref := range collectOutputSinks(spec, field.NewPath("spec")) {
		warnings = append(warnings, warnOneSinkBatchSize(ref)...)
	}
	return warnings
}

func warnOneSinkBatchSize(ref outputSinkRef) admission.Warnings {
	if ref.sink == nil || ref.sink.Config == nil || len(ref.sink.Config.Raw) == 0 {
		return nil
	}
	path := ref.path.Child("config", "batchSize").String()
	switch ref.sink.Type {
	case "postgresql":
		cfg, err := ref.sink.GetPostgreSQLConfig()
		if err != nil || cfg == nil {
			return nil
		}
		return warnBatchSizeBelow(path, cfg.BatchSize, sinkbatch.RecommendedMinPostgreSQLBatchSize, "postgresql")
	case "clickhouse":
		cfg, err := ref.sink.GetClickHouseConfig()
		if err != nil || cfg == nil {
			return nil
		}
		return warnBatchSizeBelow(path, cfg.BatchSize, sinkbatch.RecommendedMinClickHouseBatchSize, "clickhouse")
	case "trino":
		cfg, err := ref.sink.GetTrinoConfig()
		if err != nil || cfg == nil {
			return nil
		}
		var warnings admission.Warnings
		warnings = append(warnings, warnBatchSizeBelow(path, cfg.BatchSize, sinkbatch.RecommendedMinTrinoBatchSize, "trino")...)
		if cfg.UpsertMode != nil && *cfg.UpsertMode {
			warnings = append(warnings,
				fmt.Sprintf("%s: trino upsertMode uses MERGE (slower than multi-row INSERT); keep batchSize modest and set queryTimeoutSeconds to cover nextUri polling",
					ref.path.Child("config", "upsertMode")))
		}
		return warnings
	case "nessie":
		cfg, err := ref.sink.GetNessieConfig()
		if err != nil || cfg == nil {
			return nil
		}
		return warnBatchSizeBelow(path, cfg.BatchSize, sinkbatch.RecommendedMinNessieBatchSize, "nessie")
	case "iceberg":
		cfg, err := ref.sink.GetIcebergConfig()
		if err != nil || cfg == nil {
			return nil
		}
		return warnBatchSizeBelow(path, cfg.BatchSize, sinkbatch.RecommendedMinIcebergBatchSize, "iceberg")
	default:
		return nil
	}
}

func warnBatchSizeBelow(path string, batchSize *int32, recommended int32, sinkType string) admission.Warnings {
	if batchSize == nil || *batchSize <= 0 || *batchSize >= recommended {
		return nil
	}
	return admission.Warnings{
		fmt.Sprintf("%s: %s sink batchSize=%d is below recommended minimum %d for throughput; raise batchSize or use batchFlushIntervalSeconds with larger batches",
			path, sinkType, *batchSize, recommended),
	}
}

// kafkaMessageAckLargeBatchThreshold is the sink batchSize above which Kafka + message-ack
// (with collapseBatchOnMessageAck=false) warrants an admission warning to prefer batch ack.
const kafkaMessageAckLargeBatchThreshold int32 = 1

func warnKafkaMessageAckLargeBatch(spec *DataFlowSpec) admission.Warnings {
	if spec == nil || spec.Source.Type != "kafka" {
		return nil
	}
	if !AckGranularityIsMessage(spec) {
		return nil
	}
	// With collapse enabled, runtime MaxBatchSize=1 — per-mark Commit matches flush size.
	if CollapseBatchOnMessageAckOrDefault(spec) {
		return nil
	}
	for _, ref := range collectOutputSinks(spec, field.NewPath("spec")) {
		bs, ok := sinkBatchSizeForWarning(ref.sink)
		if !ok || bs <= kafkaMessageAckLargeBatchThreshold {
			continue
		}
		return admission.Warnings{
			fmt.Sprintf("spec.ackGranularity=message with Kafka source and %s batchSize=%d (collapseBatchOnMessageAck=false): prefer ackGranularity=batch so Kafka commits once per sink flush; message mode commits on every mark. Use an idempotent sink (upsertMode + conflictKey) either way",
				ref.role, bs),
		}
	}
	return nil
}

// sinkBatchSizeForWarning returns the configured batchSize, or the processor default when omitted
// for known batch-oriented sinks. ok is false when the sink has no batchSize concept / unreadable config.
func sinkBatchSizeForWarning(sink *SinkSpec) (int32, bool) {
	if sink == nil || sink.Config == nil || len(sink.Config.Raw) == 0 {
		return 0, false
	}
	switch sink.Type {
	case "postgresql":
		cfg, err := sink.GetPostgreSQLConfig()
		if err != nil || cfg == nil {
			return 0, false
		}
		if cfg.BatchSize != nil && *cfg.BatchSize > 0 {
			return *cfg.BatchSize, true
		}
		return sinkbatch.DefaultPostgreSQLBatchSize, true
	case "clickhouse":
		cfg, err := sink.GetClickHouseConfig()
		if err != nil || cfg == nil {
			return 0, false
		}
		if cfg.BatchSize != nil && *cfg.BatchSize > 0 {
			return *cfg.BatchSize, true
		}
		return sinkbatch.DefaultClickHouseBatchSize, true
	case "trino":
		cfg, err := sink.GetTrinoConfig()
		if err != nil || cfg == nil {
			return 0, false
		}
		if cfg.BatchSize != nil && *cfg.BatchSize > 0 {
			return *cfg.BatchSize, true
		}
		return sinkbatch.DefaultTrinoBatchSize, true
	case "nessie":
		cfg, err := sink.GetNessieConfig()
		if err != nil || cfg == nil {
			return 0, false
		}
		if cfg.BatchSize != nil && *cfg.BatchSize > 0 {
			return *cfg.BatchSize, true
		}
		return sinkbatch.DefaultNessieBatchSize, true
	case "iceberg":
		cfg, err := sink.GetIcebergConfig()
		if err != nil || cfg == nil {
			return 0, false
		}
		if cfg.BatchSize != nil && *cfg.BatchSize > 0 {
			return *cfg.BatchSize, true
		}
		return sinkbatch.DefaultIcebergBatchSize, true
	default:
		return 0, false
	}
}
