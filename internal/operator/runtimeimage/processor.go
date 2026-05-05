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

package runtimeimage

import (
	"os"

	"github.com/dataflow-operator/dataflow/internal/version"
)

// ProcessorImageEnv is the environment variable that overrides the processor container image.
const ProcessorImageEnv = "PROCESSOR_IMAGE"

// ProcessorImage returns the processor image: PROCESSOR_IMAGE if set, otherwise the default
// built from operator version (see version.DefaultProcessorImage).
func ProcessorImage() string {
	if img := os.Getenv(ProcessorImageEnv); img != "" {
		return img
	}
	return version.DefaultProcessorImage()
}
