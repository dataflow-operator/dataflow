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
	"github.com/stretchr/testify/require"
)

func TestValidatePostgreSQLReplicaIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		replIdent byte
		hasPK     bool
		wantError string
	}{
		{
			name:      "default with primary key",
			replIdent: 'd',
			hasPK:     true,
		},
		{
			name:      "full without primary key",
			replIdent: 'f',
			hasPK:     false,
		},
		{
			name:      "index without primary key",
			replIdent: 'i',
			hasPK:     false,
		},
		{
			name:      "default without primary key",
			replIdent: 'd',
			hasPK:     false,
			wantError: "REPLICA IDENTITY DEFAULT",
		},
		{
			name:      "nothing",
			replIdent: 'n',
			hasPK:     true,
			wantError: "REPLICA IDENTITY NOTHING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validatePostgreSQLReplicaIdentity("public.orders", tt.replIdent, tt.hasPK)
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantError)
		})
	}
}
