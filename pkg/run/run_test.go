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

package run

import (
	"context"
	"testing"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_InvalidSpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "unsupported"},
		Sink:   dataflowv1.SinkSpec{Type: "unsupported"},
	}

	err := Run(ctx, spec, RunOptions{Namespace: "ns", Name: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
}

func TestRun_NilSpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	err := Run(ctx, nil, RunOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec is required")
}

func TestRun_EmptySpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: ""},
		Sink:   dataflowv1.SinkSpec{Type: ""},
	}

	err := Run(ctx, spec, RunOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
}
