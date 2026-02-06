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

package transformers

import (
	"context"
	"encoding/json"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensure RouterTransformer implements SetLogger (used by processor).
var _ interface{ SetLogger(logr.Logger) } = (*RouterTransformer)(nil)

func TestRouterTransformer_SetLogger_DoesNotPanic(t *testing.T) {
	transformer := NewRouterTransformer(&v1.RouterTransformation{
		Routes: []v1.RouteRule{
			{Condition: "$.type == 'order'", Sink: v1.SinkSpec{Type: "kafka"}},
		},
	})
	transformer.SetLogger(logr.Discard())

	msg := types.NewMessage([]byte(`{"type":"order"}`))
	result, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Contains(t, result[0].Metadata, "routed_condition")
	assert.Equal(t, "$.type == 'order'", result[0].Metadata["routed_condition"])
}

func TestRouterTransformer_Transform_WithLogger_SameBehavior(t *testing.T) {
	transformer := NewRouterTransformer(&v1.RouterTransformation{
		Routes: []v1.RouteRule{
			{Condition: "type == 'user'", Sink: v1.SinkSpec{Type: "kafka"}},
			{Condition: "type == 'order'", Sink: v1.SinkSpec{Type: "kafka"}},
		},
	})
	transformer.SetLogger(logr.Discard())
	ctx := context.Background()

	t.Run("match first route", func(t *testing.T) {
		data, _ := json.Marshal(map[string]interface{}{"id": 1, "type": "user", "name": "alice"})
		msg := types.NewMessage(data)
		result, err := transformer.Transform(ctx, msg)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "type == 'user'", result[0].Metadata["routed_condition"])
		assert.Equal(t, data, result[0].Data)
	})

	t.Run("match second route", func(t *testing.T) {
		data, _ := json.Marshal(map[string]interface{}{"id": 2, "type": "order", "amount": 100})
		msg := types.NewMessage(data)
		result, err := transformer.Transform(ctx, msg)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "type == 'order'", result[0].Metadata["routed_condition"])
		assert.Equal(t, data, result[0].Data)
	})

	t.Run("no match returns original message", func(t *testing.T) {
		data, _ := json.Marshal(map[string]interface{}{"id": 3, "type": "unknown"})
		msg := types.NewMessage(data)
		msg.Metadata["custom"] = "value"
		result, err := transformer.Transform(ctx, msg)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.NotContains(t, result[0].Metadata, "routed_condition")
		assert.Equal(t, data, result[0].Data)
		assert.Equal(t, "value", result[0].Metadata["custom"])
	})
}
