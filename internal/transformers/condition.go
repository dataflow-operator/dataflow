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
	"strings"

	"github.com/tidwall/gjson"
)

// comparisonKind identifies a comparison operator in a condition expression.
type comparisonKind int

const (
	comparisonNone comparisonKind = iota
	comparisonEq                  // ==
	comparisonNeq                 // !=
)

// evaluateCondition evaluates a filter/router condition against JSON payload bytes.
// Supported forms:
//   - "$.field" / "field" — field exists and is truthy
//   - "$.field == 'value'" / "$.field == \"value\"" — string equality (also unquoted literals)
//   - "$.field != 'value'" — string inequality
//
// Missing fields make the condition false for both truthiness and comparisons.
func evaluateCondition(data []byte, condition string) bool {
	if idx, kind := findComparison(condition); kind != comparisonNone {
		fieldPath := normalizeFieldPath(strings.TrimSpace(condition[:idx]))
		expectedValue := extractComparisonValue(condition[idx:], kind)
		result := gjson.GetBytes(data, fieldPath)
		if !result.Exists() {
			return false
		}
		actual := result.String()
		switch kind {
		case comparisonEq:
			return actual == expectedValue
		case comparisonNeq:
			return actual != expectedValue
		default:
			return false
		}
	}

	fieldPath := normalizeFieldPath(strings.TrimSpace(condition))
	result := gjson.GetBytes(data, fieldPath)
	if !result.Exists() {
		return false
	}
	return isTruthy(result.Value())
}

// findComparison finds "==" or "!=" in the condition string.
// Returns the index of the first operator character and the operator kind, or (-1, comparisonNone).
func findComparison(condition string) (int, comparisonKind) {
	eqIdx := findOperatorAt(condition, "==")
	neqIdx := findOperatorAt(condition, "!=")

	switch {
	case eqIdx < 0 && neqIdx < 0:
		return -1, comparisonNone
	case eqIdx < 0:
		return neqIdx, comparisonNeq
	case neqIdx < 0:
		return eqIdx, comparisonEq
	case neqIdx < eqIdx:
		return neqIdx, comparisonNeq
	default:
		return eqIdx, comparisonEq
	}
}

// findOperatorAt finds op ("==" or "!=") with optional surrounding spaces / quotes after.
func findOperatorAt(condition, op string) int {
	if len(op) != 2 {
		return -1
	}
	op0, op1 := op[0], op[1]

	// Prefer " op " / " op'" / " op\"" (space before operator)
	for i := 0; i < len(condition)-2; i++ {
		if condition[i] == ' ' && condition[i+1] == op0 && condition[i+2] == op1 {
			if i+3 < len(condition) && isComparisonValueStart(condition[i+3]) {
				return i + 1
			}
		}
	}
	// Also accept "op " / "op'" / "op\"" without leading space
	for i := 1; i < len(condition)-1; i++ {
		if condition[i] == op0 && condition[i+1] == op1 {
			beforeOK := condition[i-1] != '=' && condition[i-1] != '!'
			afterOK := i+2 < len(condition) && isComparisonValueStart(condition[i+2])
			if beforeOK && afterOK {
				return i
			}
		}
	}
	return -1
}

func isComparisonValueStart(c byte) bool {
	return c == ' ' || c == '\'' || c == '"'
}

// extractComparisonValue extracts the RHS of a comparison starting at the operator.
// Supports quoted ('value' / "value") and unquoted (true, 42, active) values.
func extractComparisonValue(comparison string, kind comparisonKind) string {
	comparison = strings.TrimSpace(comparison)
	var prefix string
	switch kind {
	case comparisonEq:
		prefix = "=="
	case comparisonNeq:
		prefix = "!="
	default:
		return ""
	}
	if !strings.HasPrefix(comparison, prefix) {
		return ""
	}
	comparison = strings.TrimSpace(strings.TrimPrefix(comparison, prefix))
	if len(comparison) == 0 {
		return ""
	}

	quote := comparison[0]
	if quote == '\'' || quote == '"' {
		for i := 1; i < len(comparison); i++ {
			if comparison[i] == quote && (i == 1 || comparison[i-1] != '\\') {
				return comparison[1:i]
			}
		}
		return ""
	}
	return comparison
}
