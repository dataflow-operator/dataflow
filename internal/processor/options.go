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

package processor

import (
	"context"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/connectors"
)

// ProcessorOption configures the processor.
type ProcessorOption func(*ProcessorOptions)

// ProcessorOptions holds optional configuration for the processor.
type ProcessorOptions struct {
	CheckpointStore checkpoint.Store
}

// WithCheckpointStore enables checkpoint persistence for polling sources.
func WithCheckpointStore(store checkpoint.Store) ProcessorOption {
	return func(o *ProcessorOptions) {
		o.CheckpointStore = store
	}
}

// sourceTypes that support checkpoint persistence
var checkpointSourceTypes = map[string]bool{
	"postgresql": true,
	"clickhouse": true,
	"trino":      true,
}

// buildSourceConnectorOptions builds connector options for CreateSourceConnector.
func buildSourceConnectorOptions(ctx context.Context, sourceType string, store checkpoint.Store, channelBufferSize *int32) []connectors.SourceConnectorOption {
	var opts []connectors.SourceConnectorOption
	if store != nil && checkpointSourceTypes[sourceType] {
		opts = append(opts, connectors.WithCheckpointStore(store, sourceType))
		if data, err := connectors.LoadInitialCheckpoint(ctx, store, sourceType); err == nil && len(data) > 0 {
			opts = append(opts, connectors.WithInitialCheckpoint(data))
		}
	}
	if channelBufferSize != nil && *channelBufferSize > 0 {
		opts = append(opts, connectors.WithChannelBufferSize(int(*channelBufferSize)))
	}
	return opts
}
