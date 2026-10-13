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

func TestValidateHiveSource_requiresMetastore(t *testing.T) {
	errs := validateHiveSource(&HiveSourceSpec{}, field.NewPath("spec", "source", "config"))
	require.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "metastoreURI")
}

func TestValidateHiveSink_autoCreateRules(t *testing.T) {
	auto := true
	raw := true
	errs := validateHiveSink(&HiveSinkSpec{
		HiveEndpointSpec: HiveEndpointSpec{MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events"},
		AutoCreateTable:  &auto,
		RawMode:          &raw,
		Location:         "hdfs://nameservice/warehouse/events",
	}, field.NewPath("config"))
	require.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "nameservice")

	errs = validateHiveSink(&HiveSinkSpec{
		HiveEndpointSpec: HiveEndpointSpec{MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events"},
		AutoCreateTable:  &auto,
		Location:         "file:///tmp/events",
	}, field.NewPath("config"))
	require.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "rawMode")
}

func TestValidateHiveSink_s3RequiresCredentials(t *testing.T) {
	auto := true
	raw := true
	errs := validateHiveSink(&HiveSinkSpec{
		HiveEndpointSpec: HiveEndpointSpec{MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events"},
		AutoCreateTable:  &auto,
		RawMode:          &raw,
		Location:         "s3a://bucket/warehouse/events",
	}, field.NewPath("config"))
	require.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "accessKeySecretRef")
}
