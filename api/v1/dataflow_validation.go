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
	"encoding/json"
	"strings"

	"github.com/dataflow-operator/dataflow/internal/providers"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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

// ValidateDataFlowCronSpec validates DataFlowCron spec and returns field errors.
func ValidateDataFlowCronSpec(spec *DataFlowCronSpec) field.ErrorList {
	var all field.ErrorList
	if spec == nil {
		return all
	}
	f := field.NewPath("spec")
	all = append(all, ValidateDataFlowSpec(&spec.DataFlowSpec)...)
	if strings.TrimSpace(spec.Schedule) == "" {
		all = append(all, field.Required(f.Child("schedule"), "schedule is required"))
	} else {
		parts := strings.Fields(spec.Schedule)
		if len(parts) < 5 || len(parts) > 6 {
			all = append(all, field.Invalid(f.Child("schedule"), spec.Schedule, "cron expression must contain 5 or 6 fields"))
		}
	}
	for i, trigger := range spec.Triggers {
		tf := f.Child("triggers").Index(i)
		if strings.TrimSpace(trigger.Image) == "" {
			all = append(all, field.Required(tf.Child("image"), "image is required"))
		}
	}
	if spec.ConcurrencyPolicy != "" &&
		spec.ConcurrencyPolicy != DataFlowCronConcurrencyAllow &&
		spec.ConcurrencyPolicy != DataFlowCronConcurrencyForbid &&
		spec.ConcurrencyPolicy != DataFlowCronConcurrencyReplace {
		all = append(all, field.NotSupported(f.Child("concurrencyPolicy"), spec.ConcurrencyPolicy, []string{
			string(DataFlowCronConcurrencyAllow),
			string(DataFlowCronConcurrencyForbid),
			string(DataFlowCronConcurrencyReplace),
		}))
	}
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
	validTypes := providers.ListSourceTypes()
	validator := providers.SourceValidator(s.Type)
	if validator == nil {
		all = append(all, field.NotSupported(f.Child("type"), s.Type, validTypes))
		return all
	}
	all = append(all, validator(safeRawConfig(s.Config), f.Child("config"))...)
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
	validTypes := providers.ListSinkTypes()
	validator := providers.SinkValidator(s.Type)
	if validator == nil {
		all = append(all, field.NotSupported(f.Child("type"), s.Type, validTypes))
		return all
	}
	all = append(all, validator(safeRawConfig(s.Config), f.Child("config"))...)
	return all
}

func safeRawConfig(rawConfig *runtime.RawExtension) []byte {
	if rawConfig == nil {
		return nil
	}
	return rawConfig.Raw
}

func validateNessieSource(n *NessieSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasBaseURL := n.BaseURL != "" || n.BaseURLSecretRef != nil
	if !hasBaseURL {
		all = append(all, field.Required(f.Child("baseURL"), "baseURL or baseURLSecretRef is required"))
	}
	hasNamespace := n.Namespace != "" || n.NamespaceSecretRef != nil
	if !hasNamespace {
		all = append(all, field.Required(f.Child("namespace"), "namespace or namespaceSecretRef is required"))
	}
	hasTable := n.Table != "" || n.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if n.BaseURLSecretRef != nil {
		all = append(all, validateSecretRef(n.BaseURLSecretRef, f.Child("baseURLSecretRef"))...)
	}
	if n.NamespaceSecretRef != nil {
		all = append(all, validateSecretRef(n.NamespaceSecretRef, f.Child("namespaceSecretRef"))...)
	}
	if n.TableSecretRef != nil {
		all = append(all, validateSecretRef(n.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	if n.TokenSecretRef != nil {
		all = append(all, validateSecretRef(n.TokenSecretRef, f.Child("tokenSecretRef"))...)
	}
	all = append(all, validateNessieAuthConfig(string(n.AuthenticationType), n.BearerToken, n.TokenSecretRef, n.BasicAuth, f)...)
	return all
}

func validateNessieSink(n *NessieSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasBaseURL := n.BaseURL != "" || n.BaseURLSecretRef != nil
	if !hasBaseURL {
		all = append(all, field.Required(f.Child("baseURL"), "baseURL or baseURLSecretRef is required"))
	}
	hasNamespace := n.Namespace != "" || n.NamespaceSecretRef != nil
	if !hasNamespace {
		all = append(all, field.Required(f.Child("namespace"), "namespace or namespaceSecretRef is required"))
	}
	hasTable := n.Table != "" || n.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if n.BaseURLSecretRef != nil {
		all = append(all, validateSecretRef(n.BaseURLSecretRef, f.Child("baseURLSecretRef"))...)
	}
	if n.NamespaceSecretRef != nil {
		all = append(all, validateSecretRef(n.NamespaceSecretRef, f.Child("namespaceSecretRef"))...)
	}
	if n.TableSecretRef != nil {
		all = append(all, validateSecretRef(n.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	if n.TokenSecretRef != nil {
		all = append(all, validateSecretRef(n.TokenSecretRef, f.Child("tokenSecretRef"))...)
	}
	hasAK := n.AccessKeySecretRef != nil
	hasSK := n.SecretAccessKeySecretRef != nil
	if hasAK != hasSK {
		all = append(all, field.Invalid(f.Child("accessKeySecretRef"), n.AccessKeySecretRef, "accessKeySecretRef and secretAccessKeySecretRef must both be set or both omitted"))
	}
	if n.AccessKeySecretRef != nil {
		all = append(all, validateSecretRef(n.AccessKeySecretRef, f.Child("accessKeySecretRef"))...)
	}
	if n.SecretAccessKeySecretRef != nil {
		all = append(all, validateSecretRef(n.SecretAccessKeySecretRef, f.Child("secretAccessKeySecretRef"))...)
	}
	all = append(all, validateNessieAuthConfig(string(n.AuthenticationType), n.BearerToken, n.TokenSecretRef, n.BasicAuth, f)...)
	return all
}

func validateNessieAuthConfig(authType, bearerToken string, tokenSecretRef *SecretRef, basicAuth *BasicAuthConfig, f *field.Path) field.ErrorList {
	var all field.ErrorList

	normalized := strings.ToUpper(strings.TrimSpace(authType))
	if normalized == "" {
		normalized = string(NessieAuthenticationAuto)
	}
	allowed := []string{
		string(NessieAuthenticationAuto),
		string(NessieAuthenticationBearer),
		string(NessieAuthenticationBasic),
		string(NessieAuthenticationNone),
	}
	switch normalized {
	case string(NessieAuthenticationAuto), string(NessieAuthenticationNone):
		return all
	case string(NessieAuthenticationBearer):
		if bearerToken == "" && tokenSecretRef == nil {
			all = append(all, field.Required(f.Child("bearerToken"), "bearerToken or tokenSecretRef is required when authenticationType=BEARER"))
		}
	case string(NessieAuthenticationBasic):
		if basicAuth == nil {
			all = append(all, field.Required(f.Child("basicAuth"), "basicAuth is required when authenticationType=BASIC"))
			return all
		}
		if basicAuth.Username == "" && basicAuth.UsernameSecretRef == nil {
			all = append(all, field.Required(f.Child("basicAuth", "username"), "username or usernameSecretRef is required when authenticationType=BASIC"))
		}
		if basicAuth.Password == "" && basicAuth.PasswordSecretRef == nil {
			all = append(all, field.Required(f.Child("basicAuth", "password"), "password or passwordSecretRef is required when authenticationType=BASIC"))
		}
	default:
		all = append(all, field.NotSupported(f.Child("authenticationType"), authType, allowed))
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

func validateClickHouseSource(c *ClickHouseSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasConn := c.ConnectionString != "" || c.ConnectionStringSecretRef != nil
	if !hasConn {
		all = append(all, field.Required(f.Child("connectionString"), "connectionString or connectionStringSecretRef is required"))
	}
	hasTable := c.Table != "" || c.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if c.ConnectionStringSecretRef != nil {
		all = append(all, validateSecretRef(c.ConnectionStringSecretRef, f.Child("connectionStringSecretRef"))...)
	}
	if c.TableSecretRef != nil {
		all = append(all, validateSecretRef(c.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	return all
}

func validateClickHouseSink(c *ClickHouseSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasConn := c.ConnectionString != "" || c.ConnectionStringSecretRef != nil
	if !hasConn {
		all = append(all, field.Required(f.Child("connectionString"), "connectionString or connectionStringSecretRef is required"))
	}
	hasTable := c.Table != "" || c.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if c.ConnectionStringSecretRef != nil {
		all = append(all, validateSecretRef(c.ConnectionStringSecretRef, f.Child("connectionStringSecretRef"))...)
	}
	if c.TableSecretRef != nil {
		all = append(all, validateSecretRef(c.TableSecretRef, f.Child("tableSecretRef"))...)
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
		hasConfig := t.Config != nil && len(t.Config.Raw) > 0
		switch t.Type {
		case "timestamp":
			if hasConfig {
				var cfg TimestampTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid timestamp config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "timestamp transformation configuration is required"))
			}
		case "flatten":
			if hasConfig {
				var cfg FlattenTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid flatten config: "+err.Error()))
				} else if cfg.Field == "" {
					all = append(all, field.Required(idx.Child("config", "field"), "field is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "flatten transformation configuration is required"))
			}
		case "filter":
			if hasConfig {
				var cfg FilterTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid filter config: "+err.Error()))
				} else if cfg.Condition == "" {
					all = append(all, field.Required(idx.Child("config", "condition"), "condition is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "filter transformation configuration is required"))
			}
		case "mask":
			if hasConfig {
				var cfg MaskTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid mask config: "+err.Error()))
				} else if len(cfg.Fields) == 0 {
					all = append(all, field.Required(idx.Child("config", "fields"), "at least one field is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "mask transformation configuration is required"))
			}
		case "router":
			routerCfg, _ := t.GetRouterConfig()
			if routerCfg == nil {
				all = append(all, field.Required(idx.Child("config"), "router transformation configuration is required (config or router)"))
			} else {
				routesPath := idx.Child("router", "routes")
				if hasConfig {
					routesPath = idx.Child("config", "routes")
				}
				for j, route := range routerCfg.Routes {
					if route.Condition == "" {
						all = append(all, field.Required(routesPath.Index(j).Child("condition"), "condition is required"))
					}
					all = append(all, validateSink(&route.Sink, routesPath.Index(j).Child("sink"))...)
				}
			}
		case "select":
			if hasConfig {
				var cfg SelectTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid select config: "+err.Error()))
				} else if len(cfg.Fields) == 0 {
					all = append(all, field.Required(idx.Child("config", "fields"), "at least one field is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "select transformation configuration is required"))
			}
		case "remove":
			if hasConfig {
				var cfg RemoveTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid remove config: "+err.Error()))
				} else if len(cfg.Fields) == 0 {
					all = append(all, field.Required(idx.Child("config", "fields"), "at least one field is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "remove transformation configuration is required"))
			}
		case "snakeCase":
			if hasConfig {
				var cfg SnakeCaseTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid snakeCase config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "snakeCase transformation configuration is required"))
			}
		case "camelCase":
			if hasConfig {
				var cfg CamelCaseTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid camelCase config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "camelCase transformation configuration is required"))
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
