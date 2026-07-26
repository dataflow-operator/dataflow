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

package sinkbatch

import "testing"

func TestDefaultsMatchRecommendedMins(t *testing.T) {
	if DefaultPostgreSQLBatchSize != RecommendedMinPostgreSQLBatchSize {
		t.Fatalf("postgresql default %d != recommended min %d", DefaultPostgreSQLBatchSize, RecommendedMinPostgreSQLBatchSize)
	}
	if DefaultClickHouseBatchSize != RecommendedMinClickHouseBatchSize {
		t.Fatalf("clickhouse default %d != recommended min %d", DefaultClickHouseBatchSize, RecommendedMinClickHouseBatchSize)
	}
	if DefaultTrinoBatchSize != RecommendedMinTrinoBatchSize {
		t.Fatalf("trino default %d != recommended min %d", DefaultTrinoBatchSize, RecommendedMinTrinoBatchSize)
	}
}

func TestDefaultsPositive(t *testing.T) {
	for name, v := range map[string]int32{
		"postgresql": DefaultPostgreSQLBatchSize,
		"clickhouse": DefaultClickHouseBatchSize,
		"trino":      DefaultTrinoBatchSize,
		"nessie":     DefaultNessieBatchSize,
		"iceberg":    DefaultIcebergBatchSize,
		"flushSec":   DefaultBatchFlushIntervalSeconds,
	} {
		if v <= 0 {
			t.Errorf("%s must be positive, got %d", name, v)
		}
	}
}
