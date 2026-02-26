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

package constants

import "testing"

func TestDefaultChannelBufferSize(t *testing.T) {
	if DefaultChannelBufferSize <= 0 {
		t.Errorf("DefaultChannelBufferSize must be positive, got %d", DefaultChannelBufferSize)
	}
}

func TestDefaultSingleValueChannelBufferSize(t *testing.T) {
	if DefaultSingleValueChannelBufferSize != 1 {
		t.Errorf("DefaultSingleValueChannelBufferSize must be 1, got %d", DefaultSingleValueChannelBufferSize)
	}
}
