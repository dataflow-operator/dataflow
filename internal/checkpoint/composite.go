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

package checkpoint

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Composite is a lexicographic checkpoint (changeTime, orderByValue).
type Composite struct {
	ChangeTime   *time.Time
	OrderByValue interface{}
}

// ParseComposite reads checkpoint JSON (canonical or legacy fields).
func ParseComposite(data []byte) (Composite, error) {
	if len(data) == 0 {
		return Composite{}, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return Composite{}, err
	}

	var c Composite
	if v, ok := raw["lastReadChangeTime"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err == nil && s != "" {
			if t, err := parseCheckpointTime(s); err == nil {
				c.ChangeTime = &t
			}
		}
	}
	if v, ok := raw["lastReadOrderByValue"]; ok {
		_ = json.Unmarshal(v, &c.OrderByValue)
	}
	if c.OrderByValue == nil {
		if v, ok := raw["lastReadID"]; ok {
			_ = json.Unmarshal(v, &c.OrderByValue)
		}
	}
	if c.ChangeTime == nil {
		if v, ok := raw["lastReadTime"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err == nil && s != "" {
				if t, err := parseCheckpointTime(s); err == nil {
					c.ChangeTime = &t
				}
			}
		}
	}
	return c, nil
}

// Marshal returns canonical checkpoint JSON bytes.
func (c Composite) Marshal() []byte {
	payload := make(map[string]interface{})
	if c.ChangeTime != nil {
		payload["lastReadChangeTime"] = c.ChangeTime.UTC().Format(time.RFC3339Nano)
	}
	if c.OrderByValue != nil {
		payload["lastReadOrderByValue"] = c.OrderByValue
	}
	if len(payload) == 0 {
		return []byte("{}")
	}
	data, _ := json.Marshal(payload)
	return data
}

// ShouldAdvance reports whether next is strictly after cur in lexicographic order.
func ShouldAdvance(cur, next Composite) bool {
	if next.ChangeTime == nil && next.OrderByValue == nil {
		return false
	}
	if next.ChangeTime != nil {
		if cur.ChangeTime == nil {
			return true
		}
		if next.ChangeTime.After(*cur.ChangeTime) {
			return true
		}
		if next.ChangeTime.Equal(*cur.ChangeTime) && next.OrderByValue != nil {
			if cur.OrderByValue == nil || CompareOrderBy(next.OrderByValue, cur.OrderByValue) > 0 {
				return true
			}
		}
		return false
	}
	if next.OrderByValue != nil {
		if cur.OrderByValue == nil || CompareOrderBy(next.OrderByValue, cur.OrderByValue) > 0 {
			return true
		}
	}
	return false
}

// CompareOrderBy compares two order-by column values (-1, 0, 1).
func CompareOrderBy(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if fa, okA := toOrderByFloat64(a); okA {
		if fb, okB := toOrderByFloat64(b); okB {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			default:
				return 0
			}
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}

func toOrderByFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// NormalizeCheckpoint converts legacy checkpoint JSON to canonical form.
func NormalizeCheckpoint(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	c, err := ParseComposite(data)
	if err != nil {
		return data
	}
	if c.ChangeTime == nil && c.OrderByValue == nil {
		return data
	}
	return c.Marshal()
}

func parseCheckpointTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %q", s)
}
