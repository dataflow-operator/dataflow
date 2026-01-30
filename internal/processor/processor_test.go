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
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSourceConnector is a mock implementation of SourceConnector
type mockSourceConnector struct {
	connectErr error
	readErr    error
	messages   []*types.Message
	closeErr   error
}

func (m *mockSourceConnector) Connect(ctx context.Context) error {
	return m.connectErr
}

func (m *mockSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}

	ch := make(chan *types.Message, len(m.messages))
	for _, msg := range m.messages {
		ch <- msg
	}
	close(ch)
	return ch, nil
}

func (m *mockSourceConnector) Close() error {
	return m.closeErr
}

// mockSinkConnector is a mock implementation of SinkConnector
type mockSinkConnector struct {
	connectErr error
	writeErr   error
	closeErr   error
	messages   []*types.Message
}

func (m *mockSinkConnector) Connect(ctx context.Context) error {
	return m.connectErr
}

func (m *mockSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}

	for msg := range messages {
		m.messages = append(m.messages, msg)
	}
	return nil
}

func (m *mockSinkConnector) Close() error {
	return m.closeErr
}

func TestNewProcessor(t *testing.T) {
	tests := []struct {
		name    string
		spec    *v1.DataFlowSpec
		wantErr bool
	}{
		{
			name: "valid processor with kafka source and sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSourceSpec{
						Brokers:       []string{"localhost:9092"},
						Topic:         "test-topic",
						ConsumerGroup: "test-group",
					},
				},
				Sink: v1.SinkSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSinkSpec{
						Brokers: []string{"localhost:9092"},
						Topic:   "output-topic",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "processor with transformations",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSourceSpec{
						Brokers:       []string{"localhost:9092"},
						Topic:         "test-topic",
						ConsumerGroup: "test-group",
					},
				},
				Sink: v1.SinkSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSinkSpec{
						Brokers: []string{"localhost:9092"},
						Topic:   "output-topic",
					},
				},
				Transformations: []v1.TransformationSpec{
					{
						Type: "timestamp",
						Timestamp: &v1.TimestampTransformation{
							FieldName: "created_at",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "processor with invalid source",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "invalid",
				},
				Sink: v1.SinkSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSinkSpec{
						Brokers: []string{"localhost:9092"},
						Topic:   "output-topic",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "processor with invalid sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSourceSpec{
						Brokers:       []string{"localhost:9092"},
						Topic:         "test-topic",
						ConsumerGroup: "test-group",
					},
				},
				Sink: v1.SinkSpec{
					Type: "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewProcessor(tt.spec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, processor)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, processor)
			}
		})
	}
}

func TestProcessor_GetStats(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSourceSpec{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
		},
		Sink: v1.SinkSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
			},
		},
	}

	processor, err := NewProcessor(spec)
	require.NoError(t, err)
	require.NotNil(t, processor)

	processedCount, errorCount := processor.GetStats()
	assert.Equal(t, int64(0), processedCount)
	assert.Equal(t, int64(0), errorCount)
}

func TestProcessor_Start_SourceConnectError(t *testing.T) {
	// This test would require mocking the connectors
	// For now, we test that the processor can be created
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSourceSpec{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
		},
		Sink: v1.SinkSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
			},
		},
	}

	processor, err := NewProcessor(spec)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This will fail because we can't actually connect to Kafka
	// but we're testing that the error is handled
	err = processor.Start(ctx)
	assert.Error(t, err)
}

func TestNewProcessorWithLogger(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSourceSpec{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
		},
		Sink: v1.SinkSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "output-topic",
			},
		},
	}

	logger := logr.Discard()
	processor, err := NewProcessorWithLogger(spec, logger)
	require.NoError(t, err)
	assert.NotNil(t, processor)
}

func TestProcessor_WithRouterTransformation(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSourceSpec{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
		},
		Sink: v1.SinkSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "default-topic",
			},
		},
		Transformations: []v1.TransformationSpec{
			{
				Type: "router",
				Router: &v1.RouterTransformation{
					Routes: []v1.RouteRule{
						{
							Condition: "$.type == 'error'",
							Sink: v1.SinkSpec{
								Type: "kafka",
								Kafka: &v1.KafkaSinkSpec{
									Brokers: []string{"localhost:9092"},
									Topic:   "error-topic",
								},
							},
						},
					},
				},
			},
		},
	}

	processor, err := NewProcessor(spec)
	require.NoError(t, err)
	assert.NotNil(t, processor)

	// Verify that router sinks are stored
	// This is an internal detail, but we can check that the processor was created successfully
	assert.NotNil(t, processor)
}

func TestNewProcessor_WithErrorSink(t *testing.T) {
	tests := []struct {
		name    string
		spec    *v1.DataFlowSpec
		wantErr bool
	}{
		{
			name: "processor with error sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSourceSpec{
						Brokers:       []string{"localhost:9092"},
						Topic:         "test-topic",
						ConsumerGroup: "test-group",
					},
				},
				Sink: v1.SinkSpec{
					Type: "postgresql",
					PostgreSQL: &v1.PostgreSQLSinkSpec{
						ConnectionString: "postgres://user:pass@localhost:5432/db",
						Table:            "output_table",
					},
				},
				Errors: &v1.SinkSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSinkSpec{
						Brokers: []string{"localhost:9092"},
						Topic:   "error-topic",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "processor with invalid error sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSourceSpec{
						Brokers:       []string{"localhost:9092"},
						Topic:         "test-topic",
						ConsumerGroup: "test-group",
					},
				},
				Sink: v1.SinkSpec{
					Type: "kafka",
					Kafka: &v1.KafkaSinkSpec{
						Brokers: []string{"localhost:9092"},
						Topic:   "output-topic",
					},
				},
				Errors: &v1.SinkSpec{
					Type: "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewProcessor(tt.spec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, processor)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, processor)
			}
		})
	}
}

func TestProcessor_ErrorSinkConfiguration(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSourceSpec{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
		},
		Sink: v1.SinkSpec{
			Type: "postgresql",
			PostgreSQL: &v1.PostgreSQLSinkSpec{
				ConnectionString: "postgres://user:pass@localhost:5432/db",
				Table:            "output_table",
			},
		},
		Errors: &v1.SinkSpec{
			Type: "kafka",
			Kafka: &v1.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "error-topic",
			},
		},
	}

	processor, err := NewProcessor(spec)
	require.NoError(t, err)
	require.NotNil(t, processor)

	// Verify that processor was created with error sink configuration
	// The actual error sink connector will be created during Start()
	assert.NotNil(t, processor)
}
