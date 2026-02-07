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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Valid source types (must match connectors/factory.go).
var validSourceTypes = map[string]bool{"kafka": true, "postgresql": true, "trino": true}

// Valid sink types (must match connectors/factory.go).
var validSinkTypes = map[string]bool{"kafka": true, "postgresql": true, "trino": true}

// Valid transformation types (must match transformers/factory.go).
var validTransformationTypes = map[string]bool{
	"timestamp": true, "flatten": true, "filter": true, "mask": true,
	"router": true, "select": true, "remove": true, "snakeCase": true, "camelCase": true,
}

// ValidateDataFlowSpec validates DataFlow spec and returns a list of field errors.
func ValidateDataFlowSpec(spec *DataFlowSpec) field.ErrorList {
	var all field.ErrorList
	if spec == nil {
		return all
	}
	f := field.NewPath("spec")
	all = append(all, validateSource(&spec.Source, f.Child("source"))...)
	all = append(all, validateSink(&spec.Sink, f.Child("sink"))...)
	if spec.Errors != nil {
		all = append(all, validateSink(spec.Errors, f.Child("errors"))...)
	}
	all = append(all, validateTransformations(spec.Transformations, f.Child("transformations"))...)
	all = append(all, validateResources(spec.Resources, f.Child("resources"))...)
	return all
}

func validateSource(s *SourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if s == nil {
		all = append(all, field.Required(f, "source is required"))
		return all
	}
	if s.Type == "" {
		all = append(all, field.Required(f.Child("type"), "source type is required"))
		return all
	}
	if !validSourceTypes[s.Type] {
		all = append(all, field.NotSupported(f.Child("type"), s.Type, []string{"kafka", "postgresql", "trino"}))
		return all
	}
	switch s.Type {
	case "kafka":
		if s.Kafka == nil {
			all = append(all, field.Required(f.Child("kafka"), "kafka source configuration is required"))
		} else {
			all = append(all, validateKafkaSource(s.Kafka, f.Child("kafka"))...)
		}
	case "postgresql":
		if s.PostgreSQL == nil {
			all = append(all, field.Required(f.Child("postgresql"), "postgresql source configuration is required"))
		} else {
			all = append(all, validatePostgreSQLSource(s.PostgreSQL, f.Child("postgresql"))...)
		}
	case "trino":
		if s.Trino == nil {
			all = append(all, field.Required(f.Child("trino"), "trino source configuration is required"))
		} else {
			all = append(all, validateTrinoSource(s.Trino, f.Child("trino"))...)
		}
	}
	return all
}

func validateKafkaSource(k *KafkaSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasBrokers := len(k.Brokers) > 0 || k.BrokersSecretRef != nil
	if !hasBrokers {
		all = append(all, field.Invalid(f.Child("brokers"), k.Brokers, "brokers or brokersSecretRef is required"))
	}
	hasTopic := k.Topic != "" || k.TopicSecretRef != nil
	if !hasTopic {
		all = append(all, field.Required(f.Child("topic"), "topic or topicSecretRef is required"))
	}
	if k.BrokersSecretRef != nil {
		all = append(all, validateSecretRef(k.BrokersSecretRef, f.Child("brokersSecretRef"))...)
	}
	if k.TopicSecretRef != nil {
		all = append(all, validateSecretRef(k.TopicSecretRef, f.Child("topicSecretRef"))...)
	}
	return all
}

func validatePostgreSQLSource(p *PostgreSQLSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasConn := p.ConnectionString != "" || p.ConnectionStringSecretRef != nil
	if !hasConn {
		all = append(all, field.Required(f.Child("connectionString"), "connectionString or connectionStringSecretRef is required"))
	}
	hasTable := p.Table != "" || p.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if p.ConnectionStringSecretRef != nil {
		all = append(all, validateSecretRef(p.ConnectionStringSecretRef, f.Child("connectionStringSecretRef"))...)
	}
	if p.TableSecretRef != nil {
		all = append(all, validateSecretRef(p.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	return all
}

func validateTrinoSource(t *TrinoSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasURL := t.ServerURL != "" || t.ServerURLSecretRef != nil
	if !hasURL {
		all = append(all, field.Required(f.Child("serverURL"), "serverURL or serverURLSecretRef is required"))
	}
	hasCatalog := t.Catalog != "" || t.CatalogSecretRef != nil
	if !hasCatalog {
		all = append(all, field.Required(f.Child("catalog"), "catalog or catalogSecretRef is required"))
	}
	hasSchema := t.Schema != "" || t.SchemaSecretRef != nil
	if !hasSchema {
		all = append(all, field.Required(f.Child("schema"), "schema or schemaSecretRef is required"))
	}
	hasTable := t.Table != "" || t.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if t.ServerURLSecretRef != nil {
		all = append(all, validateSecretRef(t.ServerURLSecretRef, f.Child("serverURLSecretRef"))...)
	}
	if t.CatalogSecretRef != nil {
		all = append(all, validateSecretRef(t.CatalogSecretRef, f.Child("catalogSecretRef"))...)
	}
	if t.SchemaSecretRef != nil {
		all = append(all, validateSecretRef(t.SchemaSecretRef, f.Child("schemaSecretRef"))...)
	}
	if t.TableSecretRef != nil {
		all = append(all, validateSecretRef(t.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	return all
}

func validateSink(s *SinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if s == nil {
		all = append(all, field.Required(f, "sink is required"))
		return all
	}
	if s.Type == "" {
		all = append(all, field.Required(f.Child("type"), "sink type is required"))
		return all
	}
	if !validSinkTypes[s.Type] {
		all = append(all, field.NotSupported(f.Child("type"), s.Type, []string{"kafka", "postgresql", "trino"}))
		return all
	}
	switch s.Type {
	case "kafka":
		if s.Kafka == nil {
			all = append(all, field.Required(f.Child("kafka"), "kafka sink configuration is required"))
		} else {
			all = append(all, validateKafkaSink(s.Kafka, f.Child("kafka"))...)
		}
	case "postgresql":
		if s.PostgreSQL == nil {
			all = append(all, field.Required(f.Child("postgresql"), "postgresql sink configuration is required"))
		} else {
			all = append(all, validatePostgreSQLSink(s.PostgreSQL, f.Child("postgresql"))...)
		}
	case "trino":
		if s.Trino == nil {
			all = append(all, field.Required(f.Child("trino"), "trino sink configuration is required"))
		} else {
			all = append(all, validateTrinoSink(s.Trino, f.Child("trino"))...)
		}
	}
	return all
}

func validateKafkaSink(k *KafkaSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasBrokers := len(k.Brokers) > 0 || k.BrokersSecretRef != nil
	if !hasBrokers {
		all = append(all, field.Invalid(f.Child("brokers"), k.Brokers, "brokers or brokersSecretRef is required"))
	}
	hasTopic := k.Topic != "" || k.TopicSecretRef != nil
	if !hasTopic {
		all = append(all, field.Required(f.Child("topic"), "topic or topicSecretRef is required"))
	}
	if k.BrokersSecretRef != nil {
		all = append(all, validateSecretRef(k.BrokersSecretRef, f.Child("brokersSecretRef"))...)
	}
	if k.TopicSecretRef != nil {
		all = append(all, validateSecretRef(k.TopicSecretRef, f.Child("topicSecretRef"))...)
	}
	return all
}

func validatePostgreSQLSink(p *PostgreSQLSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasConn := p.ConnectionString != "" || p.ConnectionStringSecretRef != nil
	if !hasConn {
		all = append(all, field.Required(f.Child("connectionString"), "connectionString or connectionStringSecretRef is required"))
	}
	hasTable := p.Table != "" || p.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if p.ConnectionStringSecretRef != nil {
		all = append(all, validateSecretRef(p.ConnectionStringSecretRef, f.Child("connectionStringSecretRef"))...)
	}
	if p.TableSecretRef != nil {
		all = append(all, validateSecretRef(p.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	return all
}

func validateTrinoSink(t *TrinoSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasURL := t.ServerURL != "" || t.ServerURLSecretRef != nil
	if !hasURL {
		all = append(all, field.Required(f.Child("serverURL"), "serverURL or serverURLSecretRef is required"))
	}
	hasCatalog := t.Catalog != "" || t.CatalogSecretRef != nil
	if !hasCatalog {
		all = append(all, field.Required(f.Child("catalog"), "catalog or catalogSecretRef is required"))
	}
	hasSchema := t.Schema != "" || t.SchemaSecretRef != nil
	if !hasSchema {
		all = append(all, field.Required(f.Child("schema"), "schema or schemaSecretRef is required"))
	}
	hasTable := t.Table != "" || t.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if t.ServerURLSecretRef != nil {
		all = append(all, validateSecretRef(t.ServerURLSecretRef, f.Child("serverURLSecretRef"))...)
	}
	if t.CatalogSecretRef != nil {
		all = append(all, validateSecretRef(t.CatalogSecretRef, f.Child("catalogSecretRef"))...)
	}
	if t.SchemaSecretRef != nil {
		all = append(all, validateSecretRef(t.SchemaSecretRef, f.Child("schemaSecretRef"))...)
	}
	if t.TableSecretRef != nil {
		all = append(all, validateSecretRef(t.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	return all
}

func validateSecretRef(r *SecretRef, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if r == nil {
		return all
	}
	if r.Name == "" {
		all = append(all, field.Required(f.Child("name"), "secret name is required"))
	}
	if r.Key == "" {
		all = append(all, field.Required(f.Child("key"), "secret key is required"))
	}
	return all
}

func validateTransformations(transformations []TransformationSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	for i, t := range transformations {
		idx := f.Index(i)
		if t.Type == "" {
			all = append(all, field.Required(idx.Child("type"), "transformation type is required"))
			continue
		}
		if !validTransformationTypes[t.Type] {
			all = append(all, field.NotSupported(idx.Child("type"), t.Type,
				[]string{"timestamp", "flatten", "filter", "mask", "router", "select", "remove", "snakeCase", "camelCase"}))
			continue
		}
		switch t.Type {
		case "timestamp":
			if t.Timestamp == nil {
				all = append(all, field.Required(idx.Child("timestamp"), "timestamp transformation configuration is required"))
			}
		case "flatten":
			if t.Flatten == nil {
				all = append(all, field.Required(idx.Child("flatten"), "flatten transformation configuration is required"))
			} else if t.Flatten.Field == "" {
				all = append(all, field.Required(idx.Child("flatten", "field"), "field is required"))
			}
		case "filter":
			if t.Filter == nil {
				all = append(all, field.Required(idx.Child("filter"), "filter transformation configuration is required"))
			} else if t.Filter.Condition == "" {
				all = append(all, field.Required(idx.Child("filter", "condition"), "condition is required"))
			}
		case "mask":
			if t.Mask == nil {
				all = append(all, field.Required(idx.Child("mask"), "mask transformation configuration is required"))
			} else if len(t.Mask.Fields) == 0 {
				all = append(all, field.Required(idx.Child("mask", "fields"), "at least one field is required"))
			}
		case "router":
			if t.Router == nil {
				all = append(all, field.Required(idx.Child("router"), "router transformation configuration is required"))
			} else {
				for j, route := range t.Router.Routes {
					if route.Condition == "" {
						all = append(all, field.Required(idx.Child("router", "routes").Index(j).Child("condition"), "condition is required"))
					}
					all = append(all, validateSink(&route.Sink, idx.Child("router", "routes").Index(j).Child("sink"))...)
				}
			}
		case "select":
			if t.Select == nil {
				all = append(all, field.Required(idx.Child("select"), "select transformation configuration is required"))
			} else if len(t.Select.Fields) == 0 {
				all = append(all, field.Required(idx.Child("select", "fields"), "at least one field is required"))
			}
		case "remove":
			if t.Remove == nil {
				all = append(all, field.Required(idx.Child("remove"), "remove transformation configuration is required"))
			} else if len(t.Remove.Fields) == 0 {
				all = append(all, field.Required(idx.Child("remove", "fields"), "at least one field is required"))
			}
		case "snakeCase":
			if t.SnakeCase == nil {
				all = append(all, field.Required(idx.Child("snakeCase"), "snakeCase transformation configuration is required"))
			}
		case "camelCase":
			if t.CamelCase == nil {
				all = append(all, field.Required(idx.Child("camelCase"), "camelCase transformation configuration is required"))
			}
		}
	}
	return all
}

func validateResources(r *corev1.ResourceRequirements, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if r == nil {
		return all
	}
	if r.Limits != nil {
		for name, q := range r.Limits {
			if q.Sign() < 0 {
				all = append(all, field.Invalid(f.Child("limits").Key(string(name)), q.String(), "resource quantity must not be negative"))
			}
		}
	}
	if r.Requests != nil {
		for name, q := range r.Requests {
			if q.Sign() < 0 {
				all = append(all, field.Invalid(f.Child("requests").Key(string(name)), q.String(), "resource quantity must not be negative"))
			}
		}
	}
	return all
}
