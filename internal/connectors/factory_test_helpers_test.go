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

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sourceConnectorTestCase struct {
	name        string
	source      *v1.SourceSpec
	wantErr     bool
	errContains string
}

type sinkConnectorTestCase struct {
	name        string
	sink        *v1.SinkSpec
	wantErr     bool
	errContains string
}

func runCreateSourceConnectorTests(t *testing.T, tests []sourceConnectorTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSourceConnector(tt.source)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

func runCreateSinkConnectorTests(t *testing.T, tests []sinkConnectorTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSinkConnector(tt.sink)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}
