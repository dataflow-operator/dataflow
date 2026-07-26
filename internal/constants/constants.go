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

package constants

// DefaultChannelBufferSize is the buffer size for message channels between source/processor/sink.
const DefaultChannelBufferSize = 100

// DefaultSingleValueChannelBufferSize is the buffer size for single-value channels (errors, signals, sync).
const DefaultSingleValueChannelBufferSize = 1

// MaxBatchSizeWhenTimerOnly is the maximum batch size when flush is only by timer (batchSize=0), to avoid unbounded memory growth.
const MaxBatchSizeWhenTimerOnly = 10000

// DefaultTransformWorkers is the default number of parallel transform goroutines per processor pod.
const DefaultTransformWorkers = 1

// MaxTransformWorkers caps spec.transformWorkers to bound memory and scheduler overhead.
const MaxTransformWorkers = 64

// Sink batchSize / flush defaults live in pkg/sinkbatch (shared by connectors and admission).
