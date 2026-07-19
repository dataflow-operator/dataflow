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
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LoadTimezoneLocation loads an IANA timezone name or a fixed UTC offset (±HH:MM).
func LoadTimezoneLocation(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("timezone is required")
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc, nil
	}
	loc, err := parseFixedOffsetZone(name)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: must be IANA name or ±HH:MM offset", name)
	}
	return loc, nil
}

func parseFixedOffsetZone(s string) (*time.Location, error) {
	if len(s) != 6 || (s[0] != '+' && s[0] != '-') || s[3] != ':' {
		return nil, fmt.Errorf("invalid offset")
	}
	hours, err := strconv.Atoi(s[1:3])
	if err != nil || hours > 23 {
		return nil, fmt.Errorf("invalid hours")
	}
	mins, err := strconv.Atoi(s[4:6])
	if err != nil || mins > 59 {
		return nil, fmt.Errorf("invalid minutes")
	}
	offset := hours*3600 + mins*60
	if s[0] == '-' {
		offset = -offset
	}
	return time.FixedZone(s, offset), nil
}
