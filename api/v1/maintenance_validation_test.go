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

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateMaintenanceSpec(t *testing.T) {
	f := field.NewPath("spec").Child("maintenance")

	errs := validateMaintenance(&MaintenanceSpec{
		StartTime: "not-a-time",
		Duration:  "2h",
	}, f)
	assert.NotEmpty(t, errs)

	errs = validateMaintenance(&MaintenanceSpec{
		StartTime: "2024-01-01T02:00:00Z",
	}, f)
	assert.NotEmpty(t, errs)

	errs = validateMaintenance(&MaintenanceSpec{
		StartTime: "2024-01-01T02:00:00Z",
		Duration:  "2h",
		Repeat:    MaintenanceRepeatDaily,
		Timezone:  "Europe/Moscow",
	}, f)
	assert.Empty(t, errs)

	errs = validateMaintenance(&MaintenanceSpec{
		StartTime: "2024-01-01T02:00:00Z",
		Duration:  "2h",
		Repeat:    "yearly",
	}, f)
	assert.NotEmpty(t, errs)
}
