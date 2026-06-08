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
	"strings"

	"github.com/jackc/pgx/v5"
)

// normalizePostgreSQLConnectionString returns a connection string that pgx can parse.
// URL-form strings with special characters in the password (e.g. @, :, %) often fail
// net/url parsing; those are converted to libpq key=value format where the raw password is preserved.
func normalizePostgreSQLConnectionString(connStr string) string {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return connStr
	}
	if _, err := pgx.ParseConfig(connStr); err == nil {
		return connStr
	}
	if libpq, ok := postgreSQLURLToLibpq(connStr); ok {
		if _, err := pgx.ParseConfig(libpq); err == nil {
			return libpq
		}
	}
	return connStr
}

func postgreSQLURLToLibpq(connStr string) (string, bool) {
	scheme, rest, ok := strings.Cut(connStr, "://")
	if !ok {
		return "", false
	}
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
	default:
		return "", false
	}

	query := ""
	if idx := strings.Index(rest, "?"); idx >= 0 {
		query = rest[idx+1:]
		rest = rest[:idx]
	}

	// Split userinfo from host first: password may contain '/' or '@'.
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return "", false
	}
	userinfo := rest[:at]
	hostPath := rest[at+1:]

	dbname := ""
	hostport := hostPath
	if idx := strings.Index(hostPath, "/"); idx >= 0 {
		hostport = hostPath[:idx]
		dbname = hostPath[idx+1:]
	}

	user, password, hasPassword := strings.Cut(userinfo, ":")

	var parts []string
	host, port := hostport, ""
	if idx := strings.LastIndex(hostport, ":"); idx >= 0 {
		host = hostport[:idx]
		port = hostport[idx+1:]
	}
	if host != "" {
		parts = append(parts, "host="+host)
		if port != "" {
			parts = append(parts, "port="+port)
		}
	}
	if user != "" {
		parts = append(parts, "user="+user)
	}
	if hasPassword {
		parts = append(parts, "password="+quoteLibpqValue(password))
	}
	if dbname != "" {
		parts = append(parts, "dbname="+dbname)
	}
	for _, param := range strings.Split(query, "&") {
		param = strings.TrimSpace(param)
		if param != "" {
			parts = append(parts, param)
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, " "), true
}

func quoteLibpqValue(v string) string {
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " \t'\\") {
		return v
	}
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range v {
		switch r {
		case '\'':
			b.WriteString(`\'`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
