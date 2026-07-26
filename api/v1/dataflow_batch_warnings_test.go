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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestWarnDataFlowSpec_smallSQLBatchSizes(t *testing.T) {
	t.Parallel()

	smallPG := int32(1)
	smallCH := int32(50)
	smallTrino := int32(2)
	upsert := true

	spec := DataFlowSpec{
		Source: SourceSpec{Type: "kafka", Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b:9092"}, Topic: "t", ConsumerGroup: "g"})},
		Sink: SinkSpec{
			Type: "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{
				ConnectionString: "postgres://u:p@localhost/db",
				Table:            "out",
				BatchSize:        &smallPG,
			}),
		},
		Errors: &ErrorSinkSpec{
			SinkSpec: SinkSpec{
				Type: "clickhouse",
				Config: mustConfig(ClickHouseSinkSpec{
					ConnectionString: "clickhouse://localhost:9000",
					Table:            "err",
					BatchSize:        &smallCH,
				}),
			},
		},
		Transformations: []TransformationSpec{{
			Type: "router",
			Config: mustConfig(RouterTransformation{
				Routes: []RouteRule{{
					Condition: "true",
					Sink: SinkSpec{
						Type: "trino",
						Config: mustConfig(TrinoSinkSpec{
							ServerURL:   "http://trino:8080",
							Catalog:     "iceberg",
							Schema:      "s",
							Table:       "t",
							BatchSize:   &smallTrino,
							UpsertMode:  &upsert,
							ConflictKey: batchWarnStrPtr("id"),
						}),
					},
				}},
			}),
		}},
	}

	warnings := WarnDataFlowSpec(&spec)
	require.GreaterOrEqual(t, len(warnings), 3)
	joined := ""
	for _, w := range warnings {
		joined += w + "\n"
	}
	assert.Contains(t, joined, "postgresql")
	assert.Contains(t, joined, "clickhouse")
	assert.Contains(t, joined, "trino")
	assert.Contains(t, joined, "MERGE")
}

func TestWarnDataFlowSpec_recommendedBatchSizesNoWarn(t *testing.T) {
	t.Parallel()

	pg := RecommendedPostgreSQLSinkBatchSize
	ch := RecommendedClickHouseSinkBatchSize
	tr := RecommendedTrinoSinkBatchSize
	sinkPath := field.NewPath("spec", "sink")

	assert.Empty(t, warnOneSinkBatchSize(outputSinkRef{
		sink: &SinkSpec{
			Type: "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{
				ConnectionString: "postgres://u:p@localhost/db",
				Table:            "out",
				BatchSize:        &pg,
			}),
		},
		path: sinkPath,
	}))

	assert.Empty(t, warnOneSinkBatchSize(outputSinkRef{
		sink: &SinkSpec{
			Type: "clickhouse",
			Config: mustConfig(ClickHouseSinkSpec{
				ConnectionString: "clickhouse://localhost:9000",
				Table:            "out",
				BatchSize:        &ch,
			}),
		},
		path: sinkPath,
	}))

	assert.Empty(t, warnOneSinkBatchSize(outputSinkRef{
		sink: &SinkSpec{
			Type: "trino",
			Config: mustConfig(TrinoSinkSpec{
				ServerURL: "http://trino:8080",
				Catalog:   "hive",
				Schema:    "s",
				Table:     "t",
				BatchSize: &tr,
			}),
		},
		path: sinkPath,
	}))
}

func batchWarnStrPtr(s string) *string { return &s }

func TestWarnDataFlowSpec_kafkaMessageAckLargeBatch(t *testing.T) {
	t.Parallel()

	collapseFalse := false
	large := int32(1000)

	spec := DataFlowSpec{
		AckGranularity:            AckGranularityMessage,
		CollapseBatchOnMessageAck: &collapseFalse,
		Source:                    SourceSpec{Type: "kafka", Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b:9092"}, Topic: "t", ConsumerGroup: "g"})},
		Sink: SinkSpec{
			Type: "clickhouse",
			Config: mustConfig(ClickHouseSinkSpec{
				ConnectionString: "clickhouse://localhost:9000",
				Table:            "out",
				BatchSize:        &large,
			}),
		},
	}

	warnings := WarnDataFlowSpec(&spec)
	require.NotEmpty(t, warnings)
	joined := ""
	for _, w := range warnings {
		joined += w + "\n"
	}
	assert.Contains(t, joined, "ackGranularity=message")
	assert.Contains(t, joined, "ackGranularity=batch")
	assert.Contains(t, joined, "batchSize=1000")
}

func TestWarnDataFlowSpec_kafkaMessageAckCollapsedNoWarn(t *testing.T) {
	t.Parallel()

	large := int32(1000)
	spec := DataFlowSpec{
		AckGranularity: AckGranularityMessage,
		// collapse default true → MaxBatchSize=1 at runtime
		Source: SourceSpec{Type: "kafka", Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b:9092"}, Topic: "t", ConsumerGroup: "g"})},
		Sink: SinkSpec{
			Type: "clickhouse",
			Config: mustConfig(ClickHouseSinkSpec{
				ConnectionString: "clickhouse://localhost:9000",
				Table:            "out",
				BatchSize:        &large,
			}),
		},
	}

	warnings := WarnDataFlowSpec(&spec)
	for _, w := range warnings {
		assert.NotContains(t, w, "prefer ackGranularity=batch")
	}
}

func TestWarnDataFlowSpec_kafkaBatchAckNoMessageWarn(t *testing.T) {
	t.Parallel()

	collapseFalse := false
	large := int32(1000)
	spec := DataFlowSpec{
		AckGranularity:            AckGranularityBatch,
		CollapseBatchOnMessageAck: &collapseFalse,
		Source:                    SourceSpec{Type: "kafka", Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b:9092"}, Topic: "t", ConsumerGroup: "g"})},
		Sink: SinkSpec{
			Type: "clickhouse",
			Config: mustConfig(ClickHouseSinkSpec{
				ConnectionString: "clickhouse://localhost:9000",
				Table:            "out",
				BatchSize:        &large,
			}),
		},
	}

	warnings := WarnDataFlowSpec(&spec)
	for _, w := range warnings {
		assert.NotContains(t, w, "prefer ackGranularity=batch")
	}
}
