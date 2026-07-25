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
	data := []byte(`{"status":"active","type":"order","count":5,"enabled":true,"empty":"","zero":0,"nested":{"ok":true},"payload":{"op":"u"}}`)
	meta := map[string]interface{}{
		"topic":     "dbserver1.public.orders",
		"partition": int32(3),
		"offset":    int64(42),
	}

	tests := []struct {
		name      string
		condition string
		meta      map[string]interface{}
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

		{name: "metadata truthy", condition: "metadata.topic", meta: meta, want: true},
		{name: "metadata missing", condition: "metadata.missing", meta: meta, want: false},
		{name: "metadata eq", condition: "metadata.topic == 'dbserver1.public.orders'", meta: meta, want: true},
		{name: "metadata eq mismatch", condition: "metadata.topic == 'other'", meta: meta, want: false},
		{name: "metadata partition number", condition: "metadata.partition == 3", meta: meta, want: true},
		{name: "metadata nil map", condition: "metadata.topic", meta: nil, want: false},
		{name: "payload path named metadata uses gjson", condition: "$.payload.op == 'u'", want: true},

		{name: "and all true", condition: "$.status && $.enabled", want: true},
		{name: "and short-circuit false", condition: "$.missing && $.enabled", want: false},
		{name: "and with comparisons", condition: "$.status == 'active' && $.type == 'order'", want: true},
		{name: "and sample style", condition: "$.status && $.type && $.count", want: true},
		{name: "or first true", condition: "$.status == 'active' || $.status == 'deleted'", want: true},
		{name: "or second true", condition: "$.status == 'deleted' || $.type == 'order'", want: true},
		{name: "or both false", condition: "$.status == 'x' || $.type == 'y'", want: false},
		{name: "and tighter than or", condition: "$.missing || $.status == 'active' && $.enabled", want: true},
		{name: "mixed metadata and payload", condition: "metadata.topic == 'dbserver1.public.orders' && $.payload.op == 'u'", meta: meta, want: true},

		{name: "not truthy false", condition: "!$.empty", want: true},
		{name: "not truthy true", condition: "!$.enabled", want: false},
		{name: "not comparison", condition: "!$.status == 'deleted'", want: true},
		{name: "not neq still works", condition: "$.status != 'deleted'", want: true},
		{name: "parens or then and", condition: "($.status == 'x' || $.type == 'order') && $.enabled", want: true},
		{name: "parens group false", condition: "($.status == 'x' || $.type == 'y') && $.enabled", want: false},
		{name: "not with parens", condition: "!($.status == 'deleted')", want: true},
		{name: "and inside quotes not split", condition: `$.status == "a && b"`, want: false},
		{name: "empty condition", condition: "", want: false},
		{name: "whitespace condition", condition: "   ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, evaluateCondition(data, tt.meta, tt.condition))
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

func TestSplitTopLevel(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, splitTopLevel("a && b", "&&"))
	assert.Equal(t, []string{"a == 'x && y'", "b"}, splitTopLevel("a == 'x && y' && b", "&&"))
	assert.Equal(t, []string{"(a && b)", "c"}, splitTopLevel("(a && b) || c", "||"))
}
