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
