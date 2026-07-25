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
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// comparisonKind identifies a comparison operator in a condition expression.
type comparisonKind int

const (
	comparisonNone comparisonKind = iota
	comparisonEq                  // ==
	comparisonNeq                 // !=
)

const metadataPathPrefix = "metadata."

// evaluateCondition evaluates a filter/router/when condition against JSON payload and message metadata.
//
// Supported forms (condition engine v2):
//   - "$.field" / "field" — payload field exists and is truthy
//   - "metadata.key" — message Metadata[key] exists and is truthy
//   - "$.field == 'value'" / "metadata.topic == 'orders'" — string equality (also unquoted literals)
//   - "$.field != 'value'" — string inequality
//   - "!" unary negation: "!$.deleted", "!(metadata.topic == 'x')"
//   - "&&" / "||" with && binding tighter than ||
//   - parentheses for grouping: "(a || b) && c"
//
// Missing fields make the condition false for both truthiness and comparisons.
// Empty condition is false.
func evaluateCondition(data []byte, metadata map[string]interface{}, condition string) bool {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return false
	}
	return evalOr(data, metadata, condition)
}

func evalOr(data []byte, metadata map[string]interface{}, expr string) bool {
	parts := splitTopLevel(expr, "||")
	if len(parts) == 1 {
		return evalAnd(data, metadata, parts[0])
	}
	for _, part := range parts {
		if evalAnd(data, metadata, part) {
			return true
		}
	}
	return false
}

func evalAnd(data []byte, metadata map[string]interface{}, expr string) bool {
	parts := splitTopLevel(expr, "&&")
	if len(parts) == 1 {
		return evalUnary(data, metadata, parts[0])
	}
	for _, part := range parts {
		if !evalUnary(data, metadata, part) {
			return false
		}
	}
	return true
}

func evalUnary(data []byte, metadata map[string]interface{}, expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	// Unary ! (but not != comparison which is handled in atoms).
	if strings.HasPrefix(expr, "!") && !strings.HasPrefix(expr, "!=") {
		return !evalUnary(data, metadata, strings.TrimSpace(expr[1:]))
	}
	return evalPrimary(data, metadata, expr)
}

func evalPrimary(data []byte, metadata map[string]interface{}, expr string) bool {
	expr = strings.TrimSpace(expr)
	if len(expr) >= 2 && expr[0] == '(' {
		inner, ok := unwrapParens(expr)
		if !ok {
			return false
		}
		return evalOr(data, metadata, inner)
	}
	return evalAtom(data, metadata, expr)
}

// unwrapParens returns the content inside a single surrounding parenthesis pair
// when the closing ')' matches the opening '(' at index 0 (top-level balanced).
func unwrapParens(expr string) (string, bool) {
	if len(expr) < 2 || expr[0] != '(' {
		return "", false
	}
	depth := 0
	inQuote := byte(0)
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		if inQuote != 0 {
			if c == inQuote && !isEscaped(expr, i) {
				inQuote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if i != len(expr)-1 {
					// Trailing content after group — not a pure paren primary.
					return "", false
				}
				return strings.TrimSpace(expr[1:i]), true
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	return "", false
}

// evalAtom evaluates a single comparison or truthiness check (no && / || / ! / parens).
func evalAtom(data []byte, metadata map[string]interface{}, condition string) bool {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return false
	}

	if idx, kind := findComparison(condition); kind != comparisonNone {
		fieldPath := strings.TrimSpace(condition[:idx])
		expectedValue := extractComparisonValue(condition[idx:], kind)
		exists, _, actual := resolveConditionValue(data, metadata, fieldPath)
		if !exists {
			return false
		}
		switch kind {
		case comparisonEq:
			return actual == expectedValue
		case comparisonNeq:
			return actual != expectedValue
		default:
			return false
		}
	}

	exists, value, _ := resolveConditionValue(data, metadata, condition)
	if !exists {
		return false
	}
	return isTruthy(value)
}

// resolveConditionValue resolves a field path from message metadata or JSON payload.
// Paths starting with "metadata." read Message.Metadata; all other paths use gjson on data.
func resolveConditionValue(data []byte, metadata map[string]interface{}, path string) (exists bool, value interface{}, asString string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, nil, ""
	}

	if strings.HasPrefix(path, metadataPathPrefix) {
		key := path[len(metadataPathPrefix):]
		if key == "" || metadata == nil {
			return false, nil, ""
		}
		v, ok := metadata[key]
		if !ok || v == nil {
			return false, nil, ""
		}
		return true, v, formatConditionMetadata(v)
	}

	fieldPath := normalizeFieldPath(path)
	result := gjson.GetBytes(data, fieldPath)
	if !result.Exists() {
		return false, nil, ""
	}
	return true, result.Value(), result.String()
}

func formatConditionMetadata(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(t)
	}
}

// splitTopLevel splits expr by op ("&&" or "||") ignoring content inside quotes and parentheses.
func splitTopLevel(expr string, op string) []string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return []string{""}
	}

	var parts []string
	depth := 0
	inQuote := byte(0)
	start := 0
	opLen := len(op)

	for i := 0; i < len(expr); i++ {
		c := expr[i]
		if inQuote != 0 {
			if c == inQuote && !isEscaped(expr, i) {
				inQuote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			continue
		}
		switch c {
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		if i+opLen <= len(expr) && expr[i:i+opLen] == op {
			parts = append(parts, strings.TrimSpace(expr[start:i]))
			start = i + opLen
			i += opLen - 1
		}
	}
	parts = append(parts, strings.TrimSpace(expr[start:]))
	return parts
}

func isEscaped(s string, i int) bool {
	// Count consecutive backslashes immediately before i.
	n := 0
	for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
		n++
	}
	return n%2 == 1
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
	inQuote := byte(0)

	// Prefer " op " / " op'" / " op\"" (space before operator)
	for i := 0; i < len(condition)-2; i++ {
		c := condition[i]
		if inQuote != 0 {
			if c == inQuote && !isEscaped(condition, i) {
				inQuote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			continue
		}
		if condition[i] == ' ' && condition[i+1] == op0 && condition[i+2] == op1 {
			if i+3 < len(condition) && isComparisonValueStart(condition[i+3]) {
				return i + 1
			}
		}
	}

	inQuote = 0
	// Also accept "op " / "op'" / "op\"" without leading space
	for i := 1; i < len(condition)-1; i++ {
		c := condition[i]
		if inQuote != 0 {
			if c == inQuote && !isEscaped(condition, i) {
				inQuote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			continue
		}
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
			if comparison[i] == quote && !isEscaped(comparison, i) {
				return comparison[1:i]
			}
		}
		return ""
	}
	// Unquoted: stop at whitespace or logical operators / paren so
	// "$.a == x && $.b" atoms still extract "x" when used incorrectly;
	// atoms are already split before this runs.
	end := len(comparison)
	for i := 0; i < len(comparison); i++ {
		c := comparison[i]
		if c == ' ' || c == '\t' || c == ')' {
			end = i
			break
		}
	}
	return comparison[:end]
}
