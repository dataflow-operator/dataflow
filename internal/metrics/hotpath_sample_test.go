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

package metrics

import "testing"

func TestShouldSampleHotPathHistogram(t *testing.T) {
	prev := hotPathHistogramEvery.Load()
	t.Cleanup(func() {
		SetHotPathHistogramSampleEvery(prev)
		hotPathHistogramCounter.Store(0)
	})

	SetHotPathHistogramSampleEvery(1)
	hotPathHistogramCounter.Store(0)
	if !ShouldSampleHotPathHistogram() {
		t.Fatal("every=1 must always sample")
	}

	SetHotPathHistogramSampleEvery(4)
	hotPathHistogramCounter.Store(0)
	var hits int
	for i := 0; i < 8; i++ {
		if ShouldSampleHotPathHistogram() {
			hits++
		}
	}
	if hits != 2 {
		t.Fatalf("every=4 over 8 calls: want 2 hits, got %d", hits)
	}
}
