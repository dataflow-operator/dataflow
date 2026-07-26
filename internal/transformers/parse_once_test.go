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
	"github.com/stretchr/testify/require"
)

func TestParseOnce_ChainReusesCache(t *testing.T) {
	msg := types.NewMessage([]byte(`{"firstName":"Ada","nested":{"userId":1}}`))

	snake := NewSnakeCaseTransformer(&v1.SnakeCaseTransformation{Deep: true})
	out1, err := snake.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out1, 1)
	require.True(t, out1[0].HasCachedJSON(), "snakeCase should prime JSON cache for next stage")

	camel := NewCamelCaseTransformer(&v1.CamelCaseTransformation{Deep: true})
	out2, err := camel.Transform(context.Background(), out1[0])
	require.NoError(t, err)
	require.Len(t, out2, 1)
	require.True(t, out2[0].HasCachedJSON())

	var got map[string]interface{}
	require.NoError(t, json.Unmarshal(out2[0].Data, &got))
	require.Contains(t, got, "FirstName")
	require.Equal(t, "Ada", got["FirstName"])
}

func TestIsJSONObjectPayload(t *testing.T) {
	require.True(t, isJSONObjectPayload([]byte(`{"a":1}`)))
	require.True(t, isJSONObjectPayload([]byte("  {\"a\":1}")))
	require.False(t, isJSONObjectPayload([]byte(`[1]`)))
	require.False(t, isJSONObjectPayload([]byte(`"x"`)))
	require.False(t, isJSONObjectPayload([]byte(`not-json`)))
}

func TestInsertField_NonObjectPassesThrough(t *testing.T) {
	msg := types.NewMessage([]byte(`[1,2,3]`))
	tr := NewInsertFieldTransformer(&v1.InsertFieldTransformation{
		Fields: map[string]string{"ts": "x"},
	})
	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, msg.Data, out[0].Data)
}
