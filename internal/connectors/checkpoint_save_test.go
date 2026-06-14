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
	"errors"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type errCheckpointStore struct {
	saveErr  error
	flushErr error
}

func (s *errCheckpointStore) Load(context.Context, string) ([]byte, error) { return nil, nil }
func (s *errCheckpointStore) Save(context.Context, string, []byte) error   { return s.saveErr }
func (s *errCheckpointStore) Flush(context.Context) error                  { return s.flushErr }
func (s *errCheckpointStore) FlushAfterBatchAck(context.Context) error     { return s.flushErr }
func (s *errCheckpointStore) Clear(context.Context, string) error          { return nil }

var (
	_ checkpoint.Store          = (*errCheckpointStore)(nil)
	_ checkpoint.BatchAckSyncer = (*errCheckpointStore)(nil)
)

func TestCheckpointSaveReporter_recordsMetricOnSaveError(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{
		namespace:     "cp-save-ns",
		name:          "cp-save-flow",
		connectorType: "postgresql",
		connectorRole: "source",
	}
	saveErr := errors.New("configmap update failed")

	checkpointSaveReporter{
		logger: testr.New(t),
		meta:   &meta,
	}.report(saveErr, checkpointOpSave)

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"cp-save-ns", "cp-save-flow", "postgresql", "source", checkpointOpSave, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func connectorErrorCounterValue(t *testing.T, namespace, name, connectorType, role, operation, errorType string) float64 {
	t.Helper()
	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(namespace, name, connectorType, role, operation, errorType)
	require.NoError(t, err)
	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	return *dtoMetric.Counter.Value
}

func TestCheckpointSaveReporter_noMetricWithoutMetadata(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{connectorType: "postgresql", connectorRole: "source"}
	checkpointSaveReporter{
		logger: testr.New(t),
		meta:   &meta,
	}.report(errors.New("save failed"), checkpointOpSave)

	assert.Equal(t, 0.0, connectorErrorCounterValue(
		t, "no-meta-ns", "no-meta-flow", "postgresql", "source", checkpointOpSave, "persistence_error",
	))
}

func TestCompositeCheckpointHolder_reportsSaveError(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{
		namespace:     "cp-h-ns",
		name:          "cp-h-flow",
		connectorType: "postgresql",
		connectorRole: "source",
	}
	h := &CompositeCheckpointHolder{}
	h.InitCompositeCheckpoint(&errCheckpointStore{saveErr: errors.New("save failed")}, "postgresql", nil)
	h.setReporter(checkpointSaveReporter{logger: testr.New(t), meta: &meta})

	t1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	h.Advance(checkpoint.Composite{ChangeTime: &t1}, true)

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"cp-h-ns", "cp-h-flow", "postgresql", "source", checkpointOpSave, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func TestPostgresCDCCheckpointHolder_reportsSaveError(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{
		namespace:     "cdc-cp-ns",
		name:          "cdc-cp-flow",
		connectorType: "postgresql-cdc",
		connectorRole: "source",
	}
	var h postgresCDCCheckpointHolder
	h.init(&errCheckpointStore{saveErr: errors.New("save failed")}, "postgresql-cdc", "slot1", "pub1", nil)
	h.setReporter(checkpointSaveReporter{logger: testr.New(t), meta: &meta})

	h.setPhase(postgresCDCPhaseStreaming)

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"cdc-cp-ns", "cdc-cp-flow", "postgresql-cdc", "source", checkpointOpSave, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func TestProgressRecorder_reportsFlushError(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{
		namespace:     "flush-ns",
		name:          "flush-flow",
		connectorType: "postgresql",
		connectorRole: "sink",
	}
	rec := &progressRecorder{}
	rec.setReporter(checkpointSaveReporter{logger: testr.New(t), meta: &meta})
	rec.SetCheckpointBatchAckSyncer(&errCheckpointStore{flushErr: errors.New("flush failed")})

	rec.flushCheckpointAfterBatchAck()

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"flush-ns", "flush-flow", "postgresql", "sink", checkpointOpFlush, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func TestWireCheckpointSaveReporting_postgresqlSource(t *testing.T) {
	t.Parallel()

	p := NewPostgreSQLSourceConnectorWithOptions(&v1.PostgreSQLSourceSpec{Table: "orders"}, nil)
	p.SetMetadata("wire-ns", "wire-name")
	WireCheckpointSaveReporting(p, testr.New(t))

	p.cp.store = &errCheckpointStore{saveErr: errors.New("save failed")}
	t1 := time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC)
	p.cp.Advance(checkpoint.Composite{ChangeTime: &t1}, true)

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"wire-ns", "wire-name", "postgresql", "source", checkpointOpSave, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func TestWireCheckpointSaveReporting_postgresqlSink(t *testing.T) {
	t.Parallel()

	s := NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{Table: "orders"})
	s.SetMetadata("wire-sink-ns", "wire-sink-name")
	WireCheckpointSaveReporting(s, testr.New(t))
	s.SetCheckpointBatchAckSyncer(&errCheckpointStore{flushErr: errors.New("flush failed")})

	s.flushCheckpointAfterBatchAck()

	metric, err := metrics.ConnectorErrors.GetMetricWithLabelValues(
		"wire-sink-ns", "wire-sink-name", "postgresql", "sink", checkpointOpFlush, "persistence_error",
	)
	require.NoError(t, err)

	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Counter)
	assert.Equal(t, 1.0, *dtoMetric.Counter.Value)
}

func TestCheckpointSaveReporter_nilErrorNoOp(t *testing.T) {
	t.Parallel()

	meta := connectorMetadata{
		namespace:     "noop-ns",
		name:          "noop-flow",
		connectorType: "postgresql",
		connectorRole: "source",
	}
	checkpointSaveReporter{logger: logr.Discard(), meta: &meta}.report(nil, checkpointOpSave)

	assert.Equal(t, 0.0, connectorErrorCounterValue(
		t, "noop-ns", "noop-flow", "postgresql", "source", checkpointOpSave, "persistence_error",
	))
}
