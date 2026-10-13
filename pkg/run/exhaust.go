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

import (
	"fmt"
	"strings"

	"github.com/dataflow-operator/dataflow/pkg/providers"
)

// SourceExhausts reports whether a source closes its read channel after a snapshot
// (empty poll). The flag is Exhausts on the source registration in pkg/providers.
// Streaming sources such as kafka and postgresql-cdc do not exhaust.
func SourceExhausts(sourceType string) bool {
	return providers.SourceExhausts(sourceType)
}

func onceUnsupportedError(sourceType string) error {
	return fmt.Errorf("source type %q does not finish a one-shot run; supported sources: %s. Run without --once for continuous sources", sourceType, strings.Join(exhaustingSourceTypes(), ", "))
}

func exhaustingSourceTypes() []string {
	var types []string
	for _, sourceType := range providers.ListSourceTypes() {
		if providers.SourceExhausts(sourceType) {
			types = append(types, sourceType)
		}
	}
	return types
}
