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

package transformtypes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAll_unique_and_nonEmpty(t *testing.T) {
	t.Parallel()
	got := All()
	require.NotEmpty(t, got)
	seen := make(map[string]struct{}, len(got))
	for i, k := range got {
		require.NotEmpty(t, k, "empty key at index %d", i)
		_, dup := seen[k]
		require.False(t, dup, "duplicate key %q", k)
		seen[k] = struct{}{}
	}
}

func TestIsRegistered(t *testing.T) {
	t.Parallel()
	require.True(t, IsRegistered("timestamp"))
	require.False(t, IsRegistered("unknown"))
	require.False(t, IsRegistered(""))
}
