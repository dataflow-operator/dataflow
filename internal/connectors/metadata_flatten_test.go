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
	"testing"

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectFlattenMetadataColumnNames_AllSinks(t *testing.T) {
	msg := types.NewMessage([]byte(`{"id":1}`))
	msg.Metadata["offset"] = int64(42)
	msg.Metadata["topic"] = "events"

	cols, err := collectFlattenMetadataColumnNames([]*types.Message{msg}, "kafka_")
	require.NoError(t, err)
	assert.Equal(t, []string{"kafka_offset", "kafka_topic"}, cols)
}

func TestInferFlattenColumnCategories_IntMetadata(t *testing.T) {
	msg := types.NewMessage([]byte(`{}`))
	msg.Metadata["offset"] = int64(39700093)
	msg.Metadata["partition"] = int32(2)

	cats := inferFlattenColumnCategories([]*types.Message{msg}, []string{"kafka_offset", "kafka_partition"}, "kafka_")
	assert.Equal(t, flattenCategoryInt32, cats["kafka_offset"])
	assert.Equal(t, flattenCategoryInt32, cats["kafka_partition"])
}

func TestPostgreSQLTypeForCategory(t *testing.T) {
	assert.Equal(t, "BIGINT", postgreSQLTypeForCategory(flattenCategoryInt64))
	assert.Equal(t, "TEXT", postgreSQLTypeForCategory(flattenCategoryString))
}

func TestRawModeConfig_FlattenHelpers(t *testing.T) {
	trueVal := true
	cfg := rawModeConfig{
		RawMode:                      &trueVal,
		FlattenMetadataColumns:       &trueVal,
		FlattenMetadataColumnsPrefix: "kafka_",
	}
	assert.True(t, cfg.rawMode())
	assert.True(t, cfg.flattenMetadataColumns())
	assert.Equal(t, "kafka_", cfg.flattenMetadataPrefix())
}
