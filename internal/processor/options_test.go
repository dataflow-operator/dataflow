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

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/stretchr/testify/assert"
)

func TestBuildSourceConnectorOptions_NoStore(t *testing.T) {
	ctx := context.Background()
	opts := buildSourceConnectorOptions(ctx, "postgresql", nil)
	assert.Nil(t, opts)
}

func TestBuildSourceConnectorOptions_NonCheckpointSource(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NoopStore{}
	opts := buildSourceConnectorOptions(ctx, "kafka", store)
	assert.Nil(t, opts)
}

func TestBuildSourceConnectorOptions_PostgreSQL(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NoopStore{}
	opts := buildSourceConnectorOptions(ctx, "postgresql", store)
	assert.NotNil(t, opts)
	assert.GreaterOrEqual(t, len(opts), 1)
}
