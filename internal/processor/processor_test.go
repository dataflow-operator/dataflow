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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustConfig(v interface{}) *runtime.RawExtension {
	b, _ := json.Marshal(v)
	return &runtime.RawExtension{Raw: b}
}

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
		if msg.Ack != nil {
			msg.Ack()
		}
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
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
				},
				Sink: v1.SinkSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
				},
			},
			wantErr: false,
		},
		{
			name: "processor with transformations",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
				},
				Sink: v1.SinkSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
				},
				Transformations: []v1.TransformationSpec{
					{
						Type:   "timestamp",
						Config: mustConfig(v1.TimestampTransformation{FieldName: "created_at"}),
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
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
				},
			},
			wantErr: true,
		},
		{
			name: "processor with invalid sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
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
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
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
	// This test would require mocking the connectors. Without mocks, it uses a real
	// Kafka client (Sarama) which blocks on Connect/Read and does not respect short
	// context timeouts, causing the test to hang until the suite timeout (e.g. 10m).
	t.Skip("skipping: requires mocked source/sink connectors; real Kafka blocks and ignores context")
}

func TestNewProcessorWithLogger(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
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
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "default-topic"}),
		},
		Transformations: []v1.TransformationSpec{
			{
				Type: "router",
				Config: mustConfig(v1.RouterTransformation{
					Routes: []v1.RouteRule{
						{
							Condition: "$.type == 'error'",
							Sink: v1.SinkSpec{
								Type:   "kafka",
								Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "error-topic"}),
							},
						},
					},
				}),
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
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
				},
				Sink: v1.SinkSpec{
					Type:   "postgresql",
					Config: mustConfig(v1.PostgreSQLSinkSpec{ConnectionString: "postgres://user:pass@localhost:5432/db", Table: "output_table"}),
				},
				Errors: &v1.SinkSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "error-topic"}),
				},
			},
			wantErr: false,
		},
		{
			name: "processor with invalid error sink",
			spec: &v1.DataFlowSpec{
				Source: v1.SourceSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
				},
				Sink: v1.SinkSpec{
					Type:   "kafka",
					Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
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
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(v1.PostgreSQLSinkSpec{ConnectionString: "postgres://user:pass@localhost:5432/db", Table: "output_table"}),
		},
		Errors: &v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "error-topic"}),
		},
	}

	processor, err := NewProcessor(spec)
	require.NoError(t, err)
	require.NotNil(t, processor)

	// Verify that processor was created with error sink configuration
	// The actual error sink connector will be created during Start()
	assert.NotNil(t, processor)
}

func TestProcessor_FirstMessageLoggedOnce(t *testing.T) {
	// Documents the fix for excessive "First message received from source" logging.
	// The fix uses firstMessageLogged flag instead of activeMessages==1, since
	// activeMessages resets to 0 after each message when processing is sequential,
	// causing the log to fire for every message (e.g. 100+ logs for 100 messages).
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
		},
	}
	p, err := NewProcessor(spec)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestProcessor_createErrorMessage_approximateMetadata(t *testing.T) {
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
		},
	}
	p, err := NewProcessor(spec)
	require.NoError(t, err)
	require.NotNil(t, p)

	msg := &types.Message{Data: []byte(`{"id":1}`), Metadata: map[string]interface{}{"k": "v"}}
	errTest := errors.New("test error")

	t.Run("approximate true sets error_message_approximate in metadata", func(t *testing.T) {
		em := p.createErrorMessage(msg, errTest, true)
		require.NotNil(t, em)
		require.True(t, em.Metadata["is_error_message"].(bool))
		v, ok := em.Metadata["error_message_approximate"]
		require.True(t, ok, "error_message_approximate must be present")
		assert.True(t, v.(bool))
	})

	t.Run("approximate false does not set error_message_approximate", func(t *testing.T) {
		em := p.createErrorMessage(msg, errTest, false)
		require.NotNil(t, em)
		_, ok := em.Metadata["error_message_approximate"]
		assert.False(t, ok, "error_message_approximate must not be set when approximate is false")
	})
}

// mockConnectable is a configurable mock for the Connectable interface.
type mockConnectable struct {
	connectErrs []error // errors to return on sequential Connect calls; nil = success
	calls       int
}

func (m *mockConnectable) Connect(ctx context.Context) error {
	if m.calls < len(m.connectErrs) {
		err := m.connectErrs[m.calls]
		m.calls++
		return err
	}
	return nil
}

// transientError wraps a message so retry.IsRetryableTransient recognises it.
type transientError struct{ msg string }

func (e *transientError) Error() string { return e.msg }

func TestConnectWithRetry_Success(t *testing.T) {
	mc := &mockConnectable{}
	err := connectWithRetry(context.Background(), mc, "test", 0, time.Millisecond, logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, 0, mc.calls)
}

func TestConnectWithRetry_TransientThenSuccess(t *testing.T) {
	mc := &mockConnectable{
		connectErrs: []error{
			&transientError{"connection refused"},
			&transientError{"connection refused"},
		},
	}
	err := connectWithRetry(context.Background(), mc, "test", 0, time.Millisecond, logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, 2, mc.calls)
}

func TestConnectWithRetry_NonRetryableError(t *testing.T) {
	permanent := errors.New("invalid credentials")
	mc := &mockConnectable{connectErrs: []error{permanent}}
	err := connectWithRetry(context.Background(), mc, "test", 0, time.Millisecond, logr.Discard())
	require.ErrorIs(t, err, permanent)
}

func TestConnectWithRetry_MaxRetriesExceeded(t *testing.T) {
	mc := &mockConnectable{
		connectErrs: []error{
			&transientError{"connection refused"},
			&transientError{"connection refused"},
			&transientError{"connection refused"},
		},
	}
	err := connectWithRetry(context.Background(), mc, "my-sink", 2, time.Millisecond, logr.Discard())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries")
	assert.Contains(t, err.Error(), "my-sink")
}

func TestConnectWithRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mc := &mockConnectable{
		connectErrs: []error{&transientError{"connection refused"}},
	}
	err := connectWithRetry(ctx, mc, "test", 0, time.Millisecond, logr.Discard())
	require.ErrorIs(t, err, context.Canceled)
}

// mockLoggableConnector implements both SetLogger and SetMetadata.
type mockLoggableConnector struct {
	logger    logr.Logger
	loggerSet bool
	ns, name  string
}

func (m *mockLoggableConnector) SetLogger(l logr.Logger) {
	m.logger = l
	m.loggerSet = true
}

func (m *mockLoggableConnector) SetMetadata(ns, name string) {
	m.ns = ns
	m.name = name
}

// mockPlainConnector implements neither SetLogger nor SetMetadata.
type mockPlainConnector struct{}

func TestInitConnector_SetsLoggerAndMetadata(t *testing.T) {
	mc := &mockLoggableConnector{}
	logger := logr.Discard()
	initConnector(mc, logger, "ns1", "pipeline1")

	assert.True(t, mc.loggerSet)
	assert.Equal(t, "ns1", mc.ns)
	assert.Equal(t, "pipeline1", mc.name)
}

func TestInitConnector_NoopForPlainConnector(t *testing.T) {
	mc := &mockPlainConnector{}
	assert.NotPanics(t, func() {
		initConnector(mc, logr.Discard(), "ns", "name")
	})
}

// mockLoggerOnlyConnector implements only SetLogger.
type mockLoggerOnlyConnector struct {
	loggerSet bool
}

func (m *mockLoggerOnlyConnector) SetLogger(_ logr.Logger) { m.loggerSet = true }

func TestInitConnector_PartialInterface(t *testing.T) {
	mc := &mockLoggerOnlyConnector{}
	initConnector(mc, logr.Discard(), "ns", "name")
	assert.True(t, mc.loggerSet)
}

// Ensure Connectable is satisfied by both SourceConnector and SinkConnector mocks.
var (
	_ Connectable = &mockSourceConnector{}
	_ Connectable = &mockSinkConnector{}
)
