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

	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
	assert.Contains(t, warnings[0], "main sink")
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

func TestSinkIsIdempotent_requiresConflictKey(t *testing.T) {
	t.Parallel()

	upsertOn := true
	conflictKey := "id"

	assert.False(t, sinkIsIdempotent(&SinkSpec{
		Type:   "postgresql",
		Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn}),
	}))
	assert.True(t, sinkIsIdempotent(&SinkSpec{
		Type:   "postgresql",
		Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn, ConflictKey: &conflictKey}),
	}))
}

func TestValidateIdempotency_upsertWithoutConflictKey(t *testing.T) {
	t.Parallel()

	strict := true
	upsertOn := true
	spec := DataFlowSpec{
		StrictIdempotency: &strict,
		Source:            SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn}),
		},
	}
	errs := validateIdempotency(&spec, field.NewPath("spec"))
	assert.NotEmpty(t, errs)
}

func TestValidateIdempotency_errorSink(t *testing.T) {
	t.Parallel()

	strict := true
	upsertOn := true
	conflictKey := "id"
	spec := DataFlowSpec{
		StrictIdempotency: &strict,
		Source:            SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn, ConflictKey: &conflictKey}),
		},
		Errors: &ErrorSinkSpec{
			SinkSpec: SinkSpec{
				Type:   "postgresql",
				Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "errors"}),
			},
		},
	}
	errs := validateIdempotency(&spec, field.NewPath("spec"))
	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "error sink")
}

func TestValidateIdempotency_routerSink(t *testing.T) {
	t.Parallel()

	strict := true
	upsertOn := true
	conflictKey := "id"
	spec := DataFlowSpec{
		StrictIdempotency: &strict,
		Source:            SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn, ConflictKey: &conflictKey}),
		},
		Transformations: []TransformationSpec{
			{
				Type: transformtypes.Router,
				Config: mustConfig(RouterTransformation{
					Routes: []RouteRule{
						{
							Condition: "$.type == 'vip'",
							Sink: SinkSpec{
								Type:   "postgresql",
								Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "vip"}),
							},
						},
					},
				}),
			},
		},
	}
	errs := validateIdempotency(&spec, field.NewPath("spec"))
	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "router route sink")
}

func TestWarnDataFlowSpec_errorSinkNonIdempotent(t *testing.T) {
	t.Parallel()

	upsertOn := true
	conflictKey := "id"
	spec := DataFlowSpec{
		Source: SourceSpec{Type: "postgresql", Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "t"})},
		Sink: SinkSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "out", UpsertMode: &upsertOn, ConflictKey: &conflictKey}),
		},
		Errors: &ErrorSinkSpec{
			SinkSpec: SinkSpec{
				Type:   "postgresql",
				Config: mustConfig(PostgreSQLSinkSpec{ConnectionString: "postgres://u:p@localhost/db", Table: "errors"}),
			},
		},
	}
	warnings := WarnDataFlowSpec(&spec)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "error sink")
}
