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
)

func TestWarnDataFlowSpec_pollingNonIdempotent(t *testing.T) {
	t.Parallel()

	upsertOff := false
	spec := DataFlowSpec{
		Source: SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOff}),
		},
	}
	warnings := WarnDataFlowSpec(&spec)
	assert.Len(t, warnings, 1)
}

func TestValidateDataFlowSpec_strictIdempotency(t *testing.T) {
	t.Parallel()

	strict := true
	spec := DataFlowSpec{
		StrictIdempotency: &strict,
		Source:            SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out"}),
		},
	}
	errs := ValidateDataFlowSpec(&spec)
	assert.NotEmpty(t, errs)
}

func TestValidateErrors_ackPolicy(t *testing.T) {
	t.Parallel()

	spec := DataFlowSpec{
		Source: SourceSpec{Type: "kafka", Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"})},
		Sink:   SinkSpec{Type: "kafka", Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"})},
		Errors: &ErrorSinkSpec{
			SinkSpec:  SinkSpec{Type: "kafka", Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "err"})},
			AckPolicy: "invalid",
		},
	}
	errs := ValidateDataFlowSpec(&spec)
	assert.NotEmpty(t, errs)
}
