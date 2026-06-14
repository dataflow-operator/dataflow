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

package controller

import (
	"testing"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
)

func TestApplyCheckpointResetIntent(t *testing.T) {
	t.Parallel()

	trueVal := true
	df := &dataflowv1.DataFlow{}
	df.Annotations = map[string]string{dataflowv1.AnnotationResetCheckpoint: "true"}
	resolved := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntent(df, resolved))
	assert.NotNil(t, resolved.CheckpointReset)
	assert.True(t, *resolved.CheckpointReset)

	df2 := &dataflowv1.DataFlow{Spec: dataflowv1.DataFlowSpec{CheckpointReset: &trueVal}}
	resolved2 := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntent(df2, resolved2))
}
