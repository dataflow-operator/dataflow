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

package v1

import (
	"testing"
	"time"
)

func TestLoadTimezoneLocation(t *testing.T) {
	t.Run("IANA", func(t *testing.T) {
		loc, err := LoadTimezoneLocation("Europe/Moscow")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if loc.String() != "Europe/Moscow" {
			t.Fatalf("got %q", loc.String())
		}
	})

	t.Run("fixed offset", func(t *testing.T) {
		loc, err := LoadTimezoneLocation("+03:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, offset := time.Now().In(loc).Zone()
		if offset != 3*3600 {
			t.Fatalf("got offset %d", offset)
		}
	})

	t.Run("negative offset", func(t *testing.T) {
		loc, err := LoadTimezoneLocation("-05:30")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, offset := time.Now().In(loc).Zone()
		if offset != -5*3600-30*60 {
			t.Fatalf("got offset %d", offset)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		if _, err := LoadTimezoneLocation("Not/AZone"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("empty", func(t *testing.T) {
		if _, err := LoadTimezoneLocation(""); err == nil {
			t.Fatal("expected error")
		}
	})
}
