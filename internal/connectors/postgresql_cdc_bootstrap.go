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
	"context"
	"fmt"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/jackc/pgx/v5"
)

func replicationConnectionString(connStr string) string {
	normalized := normalizePostgreSQLConnectionString(connStr)
	if strings.Contains(normalized, "replication=") {
		return normalized
	}
	if strings.Contains(normalized, "://") {
		sep := "?"
		if strings.Contains(normalized, "?") {
			sep = "&"
		}
		return normalized + sep + "replication=database"
	}
	return normalized + " replication=database"
}

func postgresCDCPlugin(cfg *v1.PostgreSQLCDCSourceSpec) string {
	if cfg == nil || cfg.Plugin == "" {
		return "pgoutput"
	}
	return cfg.Plugin
}

func postgresCDCCreateSlot(cfg *v1.PostgreSQLCDCSourceSpec) bool {
	if cfg == nil || cfg.CreateSlotIfNotExists == nil {
		return true
	}
	return *cfg.CreateSlotIfNotExists
}

func postgresCDCCreatePublication(cfg *v1.PostgreSQLCDCSourceSpec) bool {
	if cfg == nil || cfg.CreatePublicationIfNotExists == nil {
		return true
	}
	return *cfg.CreatePublicationIfNotExists
}

func postgresCDCSnapshotMode(cfg *v1.PostgreSQLCDCSourceSpec) string {
	if cfg == nil || cfg.SnapshotMode == "" {
		return "initial"
	}
	return cfg.SnapshotMode
}

func normalizePostgreSQLTableRefs(tables []string) []string {
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !strings.Contains(t, ".") {
			t = "public." + t
		}
		out = append(out, t)
	}
	return out
}

func validatePostgreSQLReplicaIdentity(tableRef string, replIdent byte, hasPK bool) error {
	switch replIdent {
	case 'n':
		return fmt.Errorf(
			"table %s has REPLICA IDENTITY NOTHING; UPDATE/DELETE events will be incomplete — run: ALTER TABLE %s REPLICA IDENTITY FULL",
			tableRef, QuotePostgreSQLTableRef(tableRef),
		)
	case 'd':
		if !hasPK {
			return fmt.Errorf(
				"table %s has no primary key and REPLICA IDENTITY DEFAULT; UPDATE/DELETE need old row values — run: ALTER TABLE %s REPLICA IDENTITY FULL",
				tableRef, QuotePostgreSQLTableRef(tableRef),
			)
		}
	case 'f', 'i':
		return nil
	default:
		return fmt.Errorf("table %s has unsupported replica identity %q", tableRef, replIdent)
	}
	return nil
}

func checkPostgreSQLReplicaIdentity(ctx context.Context, conn *pgx.Conn, tables []string) error {
	for _, tableRef := range tables {
		schema, name := ParseTableRef(tableRef)
		var replIdent byte
		var hasPK bool
		err := conn.QueryRow(ctx, `
			SELECT c.relreplident,
			       EXISTS (
			           SELECT 1 FROM pg_constraint
			           WHERE conrelid = c.oid AND contype = 'p'
			       )
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
			schema, name,
		).Scan(&replIdent, &hasPK)
		if err != nil {
			return fmt.Errorf("check replica identity for %s: %w", tableRef, err)
		}
		if err := validatePostgreSQLReplicaIdentity(tableRef, replIdent, hasPK); err != nil {
			return err
		}
	}
	return nil
}

func checkPostgreSQLWalLevel(ctx context.Context, conn *pgx.Conn) error {
	var walLevel string
	err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return fmt.Errorf("check wal_level: %w", err)
	}
	if strings.ToLower(walLevel) != "logical" {
		return fmt.Errorf("wal_level must be logical (current: %q)", walLevel)
	}
	return nil
}

func ensurePostgreSQLPublication(ctx context.Context, conn *pgx.Conn, publicationName string, tables []string) error {
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		publicationName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if exists {
		return verifyPublicationTables(ctx, conn, publicationName, tables)
	}

	tableList := make([]string, 0, len(tables))
	for _, t := range tables {
		tableList = append(tableList, QuotePostgreSQLTableRef(t))
	}
	query := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s",
		quotePostgreSQLIdentifier(publicationName),
		strings.Join(tableList, ", "),
	)
	if _, err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("create publication %q: %w", publicationName, err)
	}
	return nil
}

func verifyPublicationTables(ctx context.Context, conn *pgx.Conn, publicationName string, tables []string) error {
	for _, table := range tables {
		schema, name := ParseTableRef(table)
		var count int
		err := conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_publication_tables
			WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
			publicationName, schema, name,
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("verify publication table %s: %w", table, err)
		}
		if count == 0 {
			return fmt.Errorf("publication %q does not include table %s", publicationName, table)
		}
	}
	return nil
}

func slotExists(ctx context.Context, conn *pgx.Conn, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		slotName,
	).Scan(&exists)
	return exists, err
}

func pgoutputPluginArgs(publicationName string) []string {
	return []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publicationName),
		"messages 'true'",
		"streaming 'true'",
	}
}
