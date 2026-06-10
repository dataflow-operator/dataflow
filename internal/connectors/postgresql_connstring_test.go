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

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLURLToLibpq(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{
			name:   "password with at sign",
			input:  "postgresql://com_devops:p@ss@db.example.net:6432/mydb?sslmode=require",
			want:   "host=db.example.net port=6432 user=com_devops password=p@ss dbname=mydb sslmode=require",
			wantOK: true,
		},
		{
			name:   "password with colon",
			input:  "postgres://user:pa:ss@localhost:5432/db?sslmode=disable",
			want:   "host=localhost port=5432 user=user password=pa:ss dbname=db sslmode=disable",
			wantOK: true,
		},
		{
			name:   "password with percent",
			input:  "postgresql://user:100%25off@localhost:5432/db",
			want:   "host=localhost port=5432 user=user password=100%25off dbname=db",
			wantOK: true,
		},
		{
			name:   "password with colon and slash",
			input:  "postgresql://user:pa:ss/w0rd@host.example.com:5432/dbname?sslmode=require",
			want:   "host=host.example.com port=5432 user=user password=pa:ss/w0rd dbname=dbname sslmode=require",
			wantOK: true,
		},
		{
			name:   "password with spaces needs quoting",
			input:  "postgresql://user:my pass@localhost:5432/db",
			want:   "host=localhost port=5432 user=user password='my pass' dbname=db",
			wantOK: true,
		},
		{
			name:   "libpq format unchanged",
			input:  "host=localhost user=u password=secret dbname=db",
			wantOK: false,
		},
		{
			name:   "no credentials",
			input:  "postgresql://localhost/db",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := postgreSQLURLToLibpq(tt.input)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNormalizePostgreSQLConnectionString(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "password with at sign",
			input: "postgresql://com_devops:Secr@t!@c-c9q3r1b3d8imi0qgqpcc.rw.mdb.yandexcloud.net:6432/mpa_content_price_stock?sslmode=require",
		},
		{
			name:  "password with colon and slash",
			input: "postgresql://user:pa:ss/w0rd@host.example.com:5432/dbname?sslmode=require",
		},
		{
			name:  "already valid url",
			input: "postgresql://user:simple@localhost:5432/db?sslmode=disable",
		},
		{
			name:  "libpq format",
			input: "host=localhost port=5432 user=user password=plain dbname=db sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized := normalizePostgreSQLConnectionString(tt.input)
			_, err := pgx.ParseConfig(normalized)
			require.NoError(t, err, "normalized=%q", normalized)
		})
	}
}

func TestNormalizePostgreSQLConnectionStringTrimsWhitespace(t *testing.T) {
	input := "  postgresql://user:p@ss@localhost:5432/db?sslmode=disable  \n"
	normalized := normalizePostgreSQLConnectionString(input)
	_, err := pgx.ParseConfig(normalized)
	require.NoError(t, err)
}
