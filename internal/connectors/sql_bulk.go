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
	"fmt"
	"strings"
)

// queuedSQL is one statement queued for a PostgreSQL sink flush.
type queuedSQL struct {
	query  string
	values []interface{}
}

// buildPostgreSQLMultiValuesInsert collapses N identical single-row INSERT … VALUES ($1…$k)
// statements into one multi-row INSERT. Returns ok=false when statements cannot be merged
// (mixed SQL, empty batch, or non-INSERT).
func buildPostgreSQLMultiValuesInsert(stmts []queuedSQL) (query string, args []interface{}, ok bool) {
	if len(stmts) == 0 {
		return "", nil, false
	}
	if len(stmts) == 1 {
		return stmts[0].query, stmts[0].values, true
	}
	base := stmts[0].query
	nArgs := len(stmts[0].values)
	if nArgs == 0 {
		return "", nil, false
	}
	for i := 1; i < len(stmts); i++ {
		if stmts[i].query != base || len(stmts[i].values) != nArgs {
			return "", nil, false
		}
	}

	valuesIdx := strings.LastIndex(strings.ToUpper(base), " VALUES ")
	if valuesIdx < 0 {
		return "", nil, false
	}
	prefix := base[:valuesIdx+len(" VALUES ")]
	// Strip trailing ON CONFLICT… from the first row's VALUES clause by taking only the
	// parenthesized placeholder group after VALUES.
	rest := base[valuesIdx+len(" VALUES "):]
	rowSuffix := ""
	parenEnd := strings.Index(rest, ")")
	if parenEnd < 0 {
		return "", nil, false
	}
	firstRow := rest[:parenEnd+1]
	if parenEnd+1 < len(rest) {
		rowSuffix = rest[parenEnd+1:] // ON CONFLICT …
	}

	var b strings.Builder
	b.WriteString(prefix)
	args = make([]interface{}, 0, len(stmts)*nArgs)
	argNum := 1
	for i, st := range stmts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		for j := 0; j < nArgs; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			cast := ""
			needle := fmt.Sprintf("$%d", j+1)
			if idx := strings.Index(firstRow, needle); idx >= 0 {
				after := firstRow[idx+len(needle):]
				if strings.HasPrefix(after, "::") {
					end := 2
					for end < len(after) && (after[end] == '_' || after[end] == '"' ||
						(after[end] >= 'a' && after[end] <= 'z') ||
						(after[end] >= 'A' && after[end] <= 'Z') ||
						(after[end] >= '0' && after[end] <= '9')) {
						end++
					}
					cast = after[:end]
				}
			}
			fmt.Fprintf(&b, "$%d%s", argNum, cast)
			argNum++
			args = append(args, st.values[j])
		}
		b.WriteByte(')')
	}
	b.WriteString(rowSuffix)
	return b.String(), args, true
}

// buildClickHouseMultiValuesInsert builds INSERT INTO t (cols) VALUES (?…),(?…) for bulk Exec.
func buildClickHouseMultiValuesInsert(table, columnsClause string, rows [][]interface{}) (query string, args []interface{}, err error) {
	if len(rows) == 0 {
		return "", nil, fmt.Errorf("empty clickhouse bulk insert")
	}
	nCols := len(rows[0])
	if nCols == 0 {
		return "", nil, fmt.Errorf("empty clickhouse row")
	}
	for i := 1; i < len(rows); i++ {
		if len(rows[i]) != nCols {
			return "", nil, fmt.Errorf("clickhouse bulk row %d has %d cols, want %d", i, len(rows[i]), nCols)
		}
	}
	phRow := "(" + strings.Repeat("?,", nCols-1) + "?)"
	placeholders := make([]string, len(rows))
	args = make([]interface{}, 0, len(rows)*nCols)
	for i, row := range rows {
		placeholders[i] = phRow
		args = append(args, row...)
	}
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, columnsClause, strings.Join(placeholders, ","))
	return query, args, nil
}

// parsePostgreSQLInsertColumns extracts column names from INSERT INTO … (cols) VALUES ….
// Returns ok=false when ON CONFLICT is present or the statement is not a plain INSERT.
func parsePostgreSQLInsertColumns(query string) (columns []string, ok bool) {
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "ON CONFLICT") {
		return nil, false
	}
	if !strings.HasPrefix(strings.TrimSpace(upper), "INSERT INTO ") {
		return nil, false
	}
	valuesIdx := strings.LastIndex(upper, " VALUES ")
	if valuesIdx < 0 {
		return nil, false
	}
	beforeValues := strings.TrimSpace(query[:valuesIdx])
	parenOpen := strings.LastIndex(beforeValues, "(")
	parenClose := strings.LastIndex(beforeValues, ")")
	if parenOpen < 0 || parenClose <= parenOpen {
		return nil, false
	}
	colPart := beforeValues[parenOpen+1 : parenClose]
	for _, c := range strings.Split(colPart, ",") {
		c = unquotePostgreSQLIdent(strings.TrimSpace(c))
		if c == "" {
			return nil, false
		}
		columns = append(columns, c)
	}
	return columns, len(columns) > 0
}

func unquotePostgreSQLIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return s
}

// canPostgreSQLCopy reports whether a homogeneous INSERT batch can use COPY FROM.
func canPostgreSQLCopy(stmts []queuedSQL) (columns []string, rows [][]any, ok bool) {
	if len(stmts) == 0 {
		return nil, nil, false
	}
	columns, ok = parsePostgreSQLInsertColumns(stmts[0].query)
	if !ok {
		return nil, nil, false
	}
	n := len(columns)
	rows = make([][]any, len(stmts))
	for i, st := range stmts {
		if st.query != stmts[0].query || len(st.values) != n {
			return nil, nil, false
		}
		row := make([]any, n)
		copy(row, st.values)
		rows[i] = row
	}
	return columns, rows, true
}
