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
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validateMaintenance(m *MaintenanceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if m == nil {
		return all
	}

	hasStart := strings.TrimSpace(m.StartTime) != ""
	hasDuration := strings.TrimSpace(m.Duration) != ""

	if hasStart != hasDuration {
		if !hasStart {
			all = append(all, field.Required(f.Child("startTime"), "startTime is required when duration is set"))
		} else {
			all = append(all, field.Required(f.Child("duration"), "duration is required when startTime is set"))
		}
	}

	if hasStart {
		if _, err := time.Parse(time.RFC3339, m.StartTime); err != nil {
			all = append(all, field.Invalid(f.Child("startTime"), m.StartTime, "must be RFC3339 timestamp"))
		}
	}

	if hasDuration {
		d, err := time.ParseDuration(m.Duration)
		if err != nil {
			all = append(all, field.Invalid(f.Child("duration"), m.Duration, "must be a valid Go duration (e.g. 2h, 30m)"))
		} else if d <= 0 {
			all = append(all, field.Invalid(f.Child("duration"), m.Duration, "must be greater than zero"))
		}
	}

	if m.Repeat != "" &&
		m.Repeat != MaintenanceRepeatDaily &&
		m.Repeat != MaintenanceRepeatWeekly &&
		m.Repeat != MaintenanceRepeatMonthly {
		all = append(all, field.NotSupported(f.Child("repeat"), string(m.Repeat), []string{
			string(MaintenanceRepeatDaily),
			string(MaintenanceRepeatWeekly),
			string(MaintenanceRepeatMonthly),
		}))
	}

	if tz := strings.TrimSpace(m.Timezone); tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			all = append(all, field.Invalid(f.Child("timezone"), m.Timezone, "must be a valid IANA timezone"))
		}
	}

	return all
}
