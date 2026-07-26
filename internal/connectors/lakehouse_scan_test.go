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

	"github.com/stretchr/testify/assert"
)

func TestLakehousePollLimitsFrom(t *testing.T) {
	t.Parallel()
	assert.Equal(t, lakehousePollLimits{}, lakehousePollLimitsFrom(nil, nil))

	rows := int32(100)
	bytes := int32(1024)
	got := lakehousePollLimitsFrom(&rows, &bytes)
	assert.Equal(t, int64(100), got.maxRows)
	assert.Equal(t, int64(1024), got.maxBytes)
	assert.True(t, got.active())

	zero := int32(0)
	gotZero := lakehousePollLimitsFrom(&zero, &zero)
	assert.False(t, gotZero.active())
}
