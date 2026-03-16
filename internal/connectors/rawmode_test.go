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
)

type rawModeTestable interface {
	rawMode() bool
}

type rawModeTestCase struct {
	name string
	sink rawModeTestable
	want bool
}

func runRawModeTests(t *testing.T, tests []rawModeTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.sink.rawMode())
		})
	}
}

func ptrBool(v bool) *bool { return &v }

func TestPostgreSQLSinkConnector_rawMode(t *testing.T) {
	runRawModeTests(t, []rawModeTestCase{
		{"nil", NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{}), false},
		{"false", NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{RawMode: ptrBool(false)}), false},
		{"true", NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{RawMode: ptrBool(true)}), true},
	})
}

func TestTrinoSinkConnector_rawMode(t *testing.T) {
	runRawModeTests(t, []rawModeTestCase{
		{"nil", NewTrinoSinkConnector(&v1.TrinoSinkSpec{}), false},
		{"false", NewTrinoSinkConnector(&v1.TrinoSinkSpec{RawMode: ptrBool(false)}), false},
		{"true", NewTrinoSinkConnector(&v1.TrinoSinkSpec{RawMode: ptrBool(true)}), true},
	})
}

func TestClickHouseSinkConnector_rawMode(t *testing.T) {
	runRawModeTests(t, []rawModeTestCase{
		{"nil", NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{}), false},
		{"false", NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{RawMode: ptrBool(false)}), false},
		{"true", NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{RawMode: ptrBool(true)}), true},
	})
}
