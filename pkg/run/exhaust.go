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

package run

import "fmt"

// SourceExhausts reports whether a source closes its read channel after a snapshot
// (empty poll). Streaming sources such as kafka and postgresql-cdc do not.
func SourceExhausts(sourceType string) bool {
	switch sourceType {
	case "postgresql", "clickhouse", "trino", "iceberg", "nessie":
		return true
	default:
		return false
	}
}

func onceUnsupportedError(sourceType string) error {
	return fmt.Errorf("source type %q does not finish a one-shot run; supported sources: postgresql, clickhouse, trino, iceberg, nessie. Run without --once for continuous sources", sourceType)
}
