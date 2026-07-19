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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvaluateCondition(t *testing.T) {
	data := []byte(`{"status":"active","type":"order","count":5,"enabled":true,"empty":"","zero":0,"nested":{"ok":true}}`)

	tests := []struct {
		name      string
		condition string
		want      bool
	}{
		{name: "truthy string", condition: "$.status", want: true},
		{name: "truthy without $", condition: "status", want: true},
		{name: "falsy empty string", condition: "$.empty", want: false},
		{name: "falsy zero", condition: "$.zero", want: false},
		{name: "truthy bool", condition: "$.enabled", want: true},
		{name: "nested truthy", condition: "$.nested.ok", want: true},
		{name: "missing field", condition: "$.missing", want: false},

		{name: "eq match single quotes", condition: "$.status == 'active'", want: true},
		{name: "eq match double quotes", condition: `$.status == "active"`, want: true},
		{name: "eq mismatch", condition: "$.status == 'deleted'", want: false},
		{name: "eq missing field", condition: "$.missing == 'x'", want: false},
		{name: "eq unquoted literal bool", condition: "$.enabled == true", want: true},
		{name: "eq unquoted number", condition: "$.count == 5", want: true},
		{name: "eq no spaces", condition: "$.status=='active'", want: true},

		{name: "neq match", condition: "$.status != 'deleted'", want: true},
		{name: "neq mismatch", condition: "$.status != 'active'", want: false},
		{name: "neq missing field", condition: "$.missing != 'x'", want: false},
		{name: "neq double quotes", condition: `$.type != "user"`, want: true},
		{name: "neq no spaces", condition: "$.status!='active'", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, evaluateCondition(data, tt.condition))
		})
	}
}

func TestFindComparison(t *testing.T) {
	tests := []struct {
		condition string
		wantIdx   int
		wantKind  comparisonKind
	}{
		{condition: "$.status == 'active'", wantIdx: 9, wantKind: comparisonEq},
		{condition: "$.status != 'deleted'", wantIdx: 9, wantKind: comparisonNeq},
		{condition: "$.status", wantIdx: -1, wantKind: comparisonNone},
		{condition: "a=='b'", wantIdx: 1, wantKind: comparisonEq},
		{condition: "a!='b'", wantIdx: 1, wantKind: comparisonNeq},
	}
	for _, tt := range tests {
		t.Run(tt.condition, func(t *testing.T) {
			idx, kind := findComparison(tt.condition)
			assert.Equal(t, tt.wantIdx, idx)
			assert.Equal(t, tt.wantKind, kind)
		})
	}
}

func TestExtractComparisonValue(t *testing.T) {
	assert.Equal(t, "active", extractComparisonValue(" == 'active'", comparisonEq))
	assert.Equal(t, "active", extractComparisonValue(` == "active"`, comparisonEq))
	assert.Equal(t, "true", extractComparisonValue(" == true", comparisonEq))
	assert.Equal(t, "deleted", extractComparisonValue(" != 'deleted'", comparisonNeq))
	assert.Equal(t, "", extractComparisonValue(" == ", comparisonEq))
	assert.Equal(t, "", extractComparisonValue(" != 'unterminated", comparisonNeq))
}
