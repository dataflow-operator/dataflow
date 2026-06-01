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

	"github.com/dataflow-operator/dataflow/internal/types"
)

// SourceConnector defines the interface for reading from a data source
type SourceConnector interface {
	// Connect establishes connection to the source
	Connect(ctx context.Context) error

	// Read returns a channel of messages from the source.
	// Connectors should close the channel when source data is exhausted for the current run.
	Read(ctx context.Context) (<-chan *types.Message, error)

	// Close closes the connection
	Close() error
}

// SourceReadErrors is implemented by sources that may report fatal read errors on a
// background goroutine after Read has returned successfully (e.g. Kafka consumer.Errors).
type SourceReadErrors interface {
	SourceConnector
	// ReadErrors returns a channel that receives at most one fatal error per Read session.
	// The channel is set when Read is called; nil before the first Read.
	ReadErrors() <-chan error
}

// SinkConnector defines the interface for writing to a data sink
type SinkConnector interface {
	// Connect establishes connection to the sink
	Connect(ctx context.Context) error

	// Write writes messages from the channel to the sink. The method returns when the channel is closed
	// or on the first fatal error. At most one error is returned per call; per-message errors are not
	// reported. Callers that need to correlate errors with specific messages must account for this
	// (e.g. treat the failed message as approximate).
	Write(ctx context.Context, messages <-chan *types.Message) error

	// Close closes the connection
	Close() error
}
