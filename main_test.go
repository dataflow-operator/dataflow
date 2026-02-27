/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this code except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"net/http"
	"os"
	"testing"
	"time"
)

func TestLeaderReadyCheck(t *testing.T) {
	t.Run("returns nil when elected channel is closed", func(t *testing.T) {
		ch := make(chan struct{})
		close(ch)
		check := leaderReadyCheck(ch)
		err := check(&http.Request{})
		if err != nil {
			t.Errorf("leaderReadyCheck with closed channel should return nil, got: %v", err)
		}
	})

	t.Run("returns error when elected channel is open", func(t *testing.T) {
		ch := make(chan struct{})
		check := leaderReadyCheck(ch)
		err := check(&http.Request{})
		if err == nil {
			t.Error("leaderReadyCheck with open channel should return non-nil error")
		}
		if err != nil && err.Error() != "not the leader" {
			t.Errorf("leaderReadyCheck error message = %q, want %q", err.Error(), "not the leader")
		}
	})
}

func TestLeaderElectionDurationFromEnv(t *testing.T) {
	const envKey = "LEADER_ELECTION_TEST_DURATION"
	restore := os.Getenv(envKey)
	defer func() {
		if restore == "" {
			os.Unsetenv(envKey)
		} else {
			os.Setenv(envKey, restore)
		}
	}()

	t.Run("returns default when env unset", func(t *testing.T) {
		os.Unsetenv(envKey)
		got := leaderElectionDurationFromEnv(envKey, 60*time.Second)
		if got != 60*time.Second {
			t.Errorf("leaderElectionDurationFromEnv(unset) = %v, want 60s", got)
		}
	})

	t.Run("returns parsed seconds when env set", func(t *testing.T) {
		os.Setenv(envKey, "30")
		got := leaderElectionDurationFromEnv(envKey, 60*time.Second)
		if got != 30*time.Second {
			t.Errorf("leaderElectionDurationFromEnv(30) = %v, want 30s", got)
		}
	})

	t.Run("returns default when env invalid", func(t *testing.T) {
		os.Setenv(envKey, "x")
		got := leaderElectionDurationFromEnv(envKey, 60*time.Second)
		if got != 60*time.Second {
			t.Errorf("leaderElectionDurationFromEnv(x) = %v, want default 60s", got)
		}
	})

	t.Run("returns default when env zero or negative", func(t *testing.T) {
		os.Setenv(envKey, "0")
		got := leaderElectionDurationFromEnv(envKey, 60*time.Second)
		if got != 60*time.Second {
			t.Errorf("leaderElectionDurationFromEnv(0) = %v, want default 60s", got)
		}
		os.Setenv(envKey, "-1")
		got = leaderElectionDurationFromEnv(envKey, 60*time.Second)
		if got != 60*time.Second {
			t.Errorf("leaderElectionDurationFromEnv(-1) = %v, want default 60s", got)
		}
	})
}
