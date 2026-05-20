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
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ptrBool(b bool) *bool { return &b }

func TestValidateFlattenMetadataSpec(t *testing.T) {
	base := NessieSinkSpec{
		BaseURL:   "http://nessie:19120",
		Namespace: "ns",
		Table:     "t",
	}

	t.Run("requires rawMode", func(t *testing.T) {
		spec := base
		spec.FlattenMetadataColumns = ptrBool(true)
		errs := validateFlattenMetadataSpec(spec.RawMode, spec.FlattenMetadataColumns, spec.FlattenMetadataColumnsPrefix, field.NewPath("sink").Child("config"))
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Error(), "requires rawMode")
	})

	t.Run("valid with rawMode and prefix", func(t *testing.T) {
		spec := base
		spec.RawMode = ptrBool(true)
		spec.FlattenMetadataColumns = ptrBool(true)
		spec.FlattenMetadataColumnsPrefix = "kafka_"
		errs := validateFlattenMetadataSpec(spec.RawMode, spec.FlattenMetadataColumns, spec.FlattenMetadataColumnsPrefix, field.NewPath("sink").Child("config"))
		assert.Empty(t, errs)
	})

	t.Run("invalid prefix character", func(t *testing.T) {
		spec := base
		spec.RawMode = ptrBool(true)
		spec.FlattenMetadataColumns = ptrBool(true)
		spec.FlattenMetadataColumnsPrefix = "kafka-"
		errs := validateFlattenMetadataSpec(spec.RawMode, spec.FlattenMetadataColumns, spec.FlattenMetadataColumnsPrefix, field.NewPath("sink").Child("config"))
		require.NotEmpty(t, errs)
	})
}
