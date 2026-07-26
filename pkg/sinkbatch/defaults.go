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

// Package sinkbatch holds shared default and recommended sink batchSize values.
// Processor connectors and admission warnings must use these constants — do not
// hardcode 100/500/10 in call sites.
package sinkbatch

// Default*BatchSize is applied by the processor when sink.config.batchSize is omitted.
const (
	DefaultPostgreSQLBatchSize int32 = 100
	DefaultClickHouseBatchSize int32 = 500
	DefaultTrinoBatchSize      int32 = 10
	DefaultNessieBatchSize     int32 = 500
	DefaultIcebergBatchSize    int32 = 500
)

// RecommendedMin*BatchSize is the admission-warning floor when batchSize is set explicitly.
// Values match the runtime defaults so omitting the field and setting it to the default behave the same.
const (
	RecommendedMinPostgreSQLBatchSize = DefaultPostgreSQLBatchSize
	RecommendedMinClickHouseBatchSize = DefaultClickHouseBatchSize
	RecommendedMinTrinoBatchSize      = DefaultTrinoBatchSize
	RecommendedMinNessieBatchSize     = DefaultNessieBatchSize
	RecommendedMinIcebergBatchSize    = DefaultIcebergBatchSize
)

// DefaultBatchFlushIntervalSeconds is the processor default when batchFlushIntervalSeconds is omitted.
const DefaultBatchFlushIntervalSeconds int32 = 10
