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

package connectors

import (
	"context"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
)

// SourceConnectorOption configures a source connector (e.g. checkpoint persistence).
type SourceConnectorOption func(*SourceConnectorOptions)

// SourceConnectorOptions holds optional configuration for source connectors.
type SourceConnectorOptions struct {
	CheckpointStore    checkpoint.Store
	SourceType         string // e.g. "postgresql", "clickhouse", "trino"
	InitialCheckpoint  []byte // pre-loaded checkpoint data from store
	ChannelBufferSize  int    // buffer size for message channels; 0 = use default
}

// WithCheckpointStore enables checkpoint persistence for the source connector.
func WithCheckpointStore(store checkpoint.Store, sourceType string) SourceConnectorOption {
	return func(o *SourceConnectorOptions) {
		o.CheckpointStore = store
		o.SourceType = sourceType
	}
}

// WithInitialCheckpoint sets pre-loaded checkpoint data (e.g. from store.Load).
func WithInitialCheckpoint(data []byte) SourceConnectorOption {
	return func(o *SourceConnectorOptions) {
		o.InitialCheckpoint = data
	}
}

// WithChannelBufferSize sets the buffer size for message channels (source→processor).
func WithChannelBufferSize(size int) SourceConnectorOption {
	return func(o *SourceConnectorOptions) {
		o.ChannelBufferSize = size
	}
}

// LoadInitialCheckpoint loads checkpoint from store for the given source type.
// Returns nil if store is nil or no checkpoint exists.
func LoadInitialCheckpoint(ctx context.Context, store checkpoint.Store, sourceType string) ([]byte, error) {
	if store == nil {
		return nil, nil
	}
	return store.Load(ctx, sourceType)
}
