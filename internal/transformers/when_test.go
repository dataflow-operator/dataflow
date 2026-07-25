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
	"k8s.io/apimachinery/pkg/runtime"
)

func TestWhenTransformer_PassthroughWhenFalse(t *testing.T) {
	cfg, err := json.Marshal(v1.MaskTransformation{
		Fields:   []string{"email"},
		MaskChar: "*",
	})
	require.NoError(t, err)

	transformer, err := CreateTransformer(&v1.TransformationSpec{
		Type:   "mask",
		When:   "metadata.topic == 'orders'",
		Config: &runtime.RawExtension{Raw: cfg},
	})
	require.NoError(t, err)

	msg := types.NewMessage([]byte(`{"email":"a@b.c"}`))
	msg.Metadata["topic"] = "users"

	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.JSONEq(t, `{"email":"a@b.c"}`, string(out[0].Data))
}

func TestWhenTransformer_AppliesWhenTrue(t *testing.T) {
	cfg, err := json.Marshal(v1.MaskTransformation{
		Fields:   []string{"email"},
		MaskChar: "*",
	})
	require.NoError(t, err)

	transformer, err := CreateTransformer(&v1.TransformationSpec{
		Type:   "mask",
		When:   "metadata.topic == 'orders'",
		Config: &runtime.RawExtension{Raw: cfg},
	})
	require.NoError(t, err)

	msg := types.NewMessage([]byte(`{"email":"a@b.c"}`))
	msg.Metadata["topic"] = "orders"

	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.JSONEq(t, `{"email":"***"}`, string(out[0].Data))
}

func TestWhenTransformer_EmptyWhenAlwaysApplies(t *testing.T) {
	cfg, err := json.Marshal(v1.FilterTransformation{Condition: "$.keep"})
	require.NoError(t, err)

	transformer, err := CreateTransformer(&v1.TransformationSpec{
		Type:   "filter",
		Config: &runtime.RawExtension{Raw: cfg},
	})
	require.NoError(t, err)
	_, ok := transformer.(*whenTransformer)
	assert.False(t, ok, "empty when should not wrap")

	msg := types.NewMessage([]byte(`{"keep":false}`))
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestWhenTransformer_OrPayloadCondition(t *testing.T) {
	cfg, err := json.Marshal(v1.SelectTransformation{Fields: []string{"id"}})
	require.NoError(t, err)

	transformer, err := CreateTransformer(&v1.TransformationSpec{
		Type:   "select",
		When:   "$.payload.op == 'u' || $.payload.op == 'c'",
		Config: &runtime.RawExtension{Raw: cfg},
	})
	require.NoError(t, err)

	t.Run("matches update", func(t *testing.T) {
		msg := types.NewMessage([]byte(`{"id":1,"payload":{"op":"u"},"extra":true}`))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.JSONEq(t, `{"id":1}`, string(out[0].Data))
	})

	t.Run("skips delete", func(t *testing.T) {
		msg := types.NewMessage([]byte(`{"id":1,"payload":{"op":"d"},"extra":true}`))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.JSONEq(t, `{"id":1,"payload":{"op":"d"},"extra":true}`, string(out[0].Data))
	})
}

func TestWhenTransformer_ForwardsSetLogger(t *testing.T) {
	cfg, err := json.Marshal(v1.RouterTransformation{
		Routes: []v1.RouteRule{
			{Condition: "$.type == 'order'", Sink: v1.SinkSpec{Type: "kafka"}},
		},
	})
	require.NoError(t, err)

	transformer, err := CreateTransformer(&v1.TransformationSpec{
		Type:   "router",
		When:   "metadata.topic == 'events'",
		Config: &runtime.RawExtension{Raw: cfg},
	})
	require.NoError(t, err)

	lc, ok := transformer.(interface{ SetLogger(logr.Logger) })
	require.True(t, ok)
	assert.NotPanics(t, func() { lc.SetLogger(logr.Discard()) })

	msg := types.NewMessage([]byte(`{"type":"order"}`))
	msg.Metadata["topic"] = "events"
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "$.type == 'order'", out[0].Metadata["routed_condition"])
}

func TestFilterTransformer_AndMetadata(t *testing.T) {
	transformer := NewFilterTransformer(&v1.FilterTransformation{
		Condition: "metadata.topic == 'orders' && $.active",
	})

	msg := types.NewMessage([]byte(`{"active":true}`))
	msg.Metadata["topic"] = "orders"
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	assert.Len(t, out, 1)

	msg.Metadata["topic"] = "users"
	out, err = transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestRouterTransformer_MetadataCondition(t *testing.T) {
	transformer := NewRouterTransformer(&v1.RouterTransformation{
		Routes: []v1.RouteRule{
			{Condition: "metadata.topic == 'orders' && $.level == 'error'", Sink: v1.SinkSpec{Type: "kafka"}},
		},
	})

	msg := types.NewMessage([]byte(`{"level":"error"}`))
	msg.Metadata["topic"] = "orders"
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "metadata.topic == 'orders' && $.level == 'error'", out[0].Metadata["routed_condition"])
}
