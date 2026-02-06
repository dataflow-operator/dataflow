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

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultProcessorImage(t *testing.T) {
	img := DefaultProcessorImage()
	assert.True(t, strings.HasPrefix(img, DefaultProcessorImageRepository+":"), "image should be repo:tag")
	assert.NotEmpty(t, Version, "Version should be set (dev when not built with ldflags)")
	assert.Equal(t, DefaultProcessorImageRepository+":"+Version, img)
}

func TestVersionNonEmpty(t *testing.T) {
	// При сборке без ldflags Version = "dev"
	assert.NotEmpty(t, Version)
}
