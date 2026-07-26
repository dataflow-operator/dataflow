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
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/types"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLCDC_reportChannelFill(t *testing.T) {
	t.Parallel()

	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		ConnectionString: "postgres://localhost/db",
		Tables:           []string{"orders"},
	})
	c.SetMetadata("cdc-ns", "cdc-flow")

	ch := make(chan *types.Message, 4)
	ch <- types.NewMessage([]byte(`{}`))
	ch <- types.NewMessage([]byte(`{}`))

	c.reportChannelFill(ch, "source")

	metric, err := metrics.ChannelFillRatio.GetMetricWithLabelValues("cdc-ns", "cdc-flow", "source")
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, metric.Write(&m))
	require.NotNil(t, m.Gauge)
	assert.InDelta(t, 0.5, *m.Gauge.Value, 0.001)
}

func TestPostgreSQLCDC_sendCDCMessageUpdatesFill(t *testing.T) {
	t.Parallel()

	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		ConnectionString: "postgres://localhost/db",
		Tables:           []string{"orders"},
	})
	c.SetMetadata("cdc-ns2", "cdc-flow2")

	ch := make(chan *types.Message, 2)
	msg := types.NewMessage([]byte(`{"id":1}`))
	require.NoError(t, c.sendCDCMessage(context.Background(), ch, msg))

	metric, err := metrics.ChannelFillRatio.GetMetricWithLabelValues("cdc-ns2", "cdc-flow2", "source")
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, metric.Write(&m))
	require.NotNil(t, m.Gauge)
	assert.InDelta(t, 0.5, *m.Gauge.Value, 0.001)
}
