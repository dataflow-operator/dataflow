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

package version

// Version задаётся при сборке через -ldflags "-X github.com/dataflow-operator/dataflow/internal/version.Version=...".
// Если не задана — остаётся "dev" (локальная сборка).
var Version = "dev"

// DefaultProcessorImageRepository — репозиторий образа процессора по умолчанию (тот же, что и оператор).
const DefaultProcessorImageRepository = "ghcr.io/dataflow-operator/dataflow"

// DefaultProcessorImage возвращает образ процессора по умолчанию: репозиторий + версия оператора.
func DefaultProcessorImage() string {
	return DefaultProcessorImageRepository + ":" + Version
}
