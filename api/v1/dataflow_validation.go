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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/dataflow-operator/dataflow/pkg/providers"
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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
		all = append(all, validateErrors(spec.Errors, f.Child("errors"))...)
	}
	all = append(all, validateTransformations(spec.Transformations, f.Child("transformations"))...)
	all = append(all, validateResources(spec.Resources, f.Child("resources"))...)
	all = append(all, validateReplicas(spec, f)...)
	all = append(all, validateAckGranularity(spec, f)...)
	all = append(all, validateIdempotency(spec, f)...)
	all = append(all, validateMaintenance(spec.Maintenance, f.Child("maintenance"))...)
	return all
}

func validateAckGranularity(spec *DataFlowSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if spec == nil || spec.AckGranularity == "" {
		return all
	}
	switch spec.AckGranularity {
	case AckGranularityBatch, AckGranularityMessage:
		return all
	default:
		return field.ErrorList{
			field.NotSupported(f.Child("ackGranularity"), spec.AckGranularity, []string{
				AckGranularityBatch,
				AckGranularityMessage,
			}),
		}
	}
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
	if replicas := effectiveReplicas(spec.Replicas); replicas > 1 {
		all = append(all, field.Invalid(f.Child("replicas"), replicas,
			"replicas greater than 1 is not supported for DataFlowCron (one processor Job per schedule tick)"))
	}
	return all
}

func effectiveReplicas(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func validateReplicas(spec *DataFlowSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if spec == nil {
		return all
	}
	replicas := effectiveReplicas(spec.Replicas)
	if replicas <= 1 {
		return all
	}
	if spec.Source.Type != "kafka" {
		all = append(all, field.Invalid(f.Child("replicas"), replicas,
			"horizontal scaling (replicas > 1) is only supported for Kafka sources; use resources or channelBufferSize for polling sources"))
		return all
	}
	if providers.SourceValidator(spec.Source.Type) == nil {
		all = append(all, field.Invalid(f.Child("replicas"), replicas,
			"horizontal scaling (replicas > 1) is not supported for plugin sources"))
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
	all = append(all, validateKafkaSecurityProtocol(k.SecurityProtocol, k.TLS, k.SASL, f.Child("securityProtocol"))...)
	all = append(all, validateKafkaConsumerTiming(k, f)...)
	return all
}

func validateKafkaConsumerTiming(k *KafkaSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	if k.ConsumerMaxWait != nil && k.ConsumerMaxWait.Duration <= 0 {
		all = append(all, field.Invalid(f.Child("consumerMaxWait"), k.ConsumerMaxWait.Duration.String(), "must be greater than zero"))
	}
	if k.NetReadTimeout != nil && k.NetReadTimeout.Duration <= 0 {
		all = append(all, field.Invalid(f.Child("netReadTimeout"), k.NetReadTimeout.Duration.String(), "must be greater than zero"))
	}
	if k.NetWriteTimeout != nil && k.NetWriteTimeout.Duration <= 0 {
		all = append(all, field.Invalid(f.Child("netWriteTimeout"), k.NetWriteTimeout.Duration.String(), "must be greater than zero"))
	}
	if k.FetchMinBytes != nil && *k.FetchMinBytes < 0 {
		all = append(all, field.Invalid(f.Child("fetchMinBytes"), *k.FetchMinBytes, "must be greater than or equal to zero"))
	}
	if k.FetchMaxBytes != nil && *k.FetchMaxBytes <= 0 {
		all = append(all, field.Invalid(f.Child("fetchMaxBytes"), *k.FetchMaxBytes, "must be greater than zero"))
	}
	if k.MaxPartitionFetchBytes != nil && *k.MaxPartitionFetchBytes <= 0 {
		all = append(all, field.Invalid(f.Child("maxPartitionFetchBytes"), *k.MaxPartitionFetchBytes, "must be greater than zero"))
	}
	if k.ConsumerMaxWait != nil && k.NetReadTimeout != nil && k.NetReadTimeout.Duration <= k.ConsumerMaxWait.Duration {
		all = append(all, field.Invalid(f.Child("netReadTimeout"), k.NetReadTimeout.Duration.String(),
			"must be greater than consumerMaxWait"))
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
	if err := validateSQLIdentifier(p.OrderByColumn, f.Child("orderByColumn")); err != nil {
		all = append(all, err)
	}
	if err := validateSQLIdentifier(p.ChangeTrackingColumn, f.Child("changeTrackingColumn")); err != nil {
		all = append(all, err)
	}
	return all
}

var postgresTableRefRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

func validatePostgreSQLCDCSource(p *PostgreSQLCDCSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasConn := p.ConnectionString != "" || p.ConnectionStringSecretRef != nil
	if !hasConn {
		all = append(all, field.Required(f.Child("connectionString"), "connectionString or connectionStringSecretRef is required"))
	}
	hasSlot := p.SlotName != "" || p.SlotNameSecretRef != nil
	if !hasSlot {
		all = append(all, field.Required(f.Child("slotName"), "slotName or slotNameSecretRef is required"))
	}
	hasPub := p.PublicationName != "" || p.PublicationNameSecretRef != nil
	if !hasPub {
		all = append(all, field.Required(f.Child("publicationName"), "publicationName or publicationNameSecretRef is required"))
	}
	if len(p.Tables) == 0 {
		all = append(all, field.Required(f.Child("tables"), "at least one table is required"))
	}
	for i, table := range p.Tables {
		if err := validatePostgreSQLTableRef(table, f.Child("tables").Index(i)); err != nil {
			all = append(all, err)
		}
	}
	if p.SnapshotMode != "" && p.SnapshotMode != "initial" && p.SnapshotMode != "never" && p.SnapshotMode != "always" {
		all = append(all, field.NotSupported(f.Child("snapshotMode"), p.SnapshotMode, []string{"initial", "never", "always"}))
	}
	if p.Plugin != "" && p.Plugin != "pgoutput" {
		all = append(all, field.NotSupported(f.Child("plugin"), p.Plugin, []string{"pgoutput"}))
	}
	if p.EnvelopeFormat != "" && p.EnvelopeFormat != "row" && p.EnvelopeFormat != "debezium" {
		all = append(all, field.NotSupported(f.Child("envelopeFormat"), p.EnvelopeFormat, []string{"row", "debezium"}))
	}
	if p.ConnectionStringSecretRef != nil {
		all = append(all, validateSecretRef(p.ConnectionStringSecretRef, f.Child("connectionStringSecretRef"))...)
	}
	if p.SlotNameSecretRef != nil {
		all = append(all, validateSecretRef(p.SlotNameSecretRef, f.Child("slotNameSecretRef"))...)
	}
	if p.PublicationNameSecretRef != nil {
		all = append(all, validateSecretRef(p.PublicationNameSecretRef, f.Child("publicationNameSecretRef"))...)
	}
	if err := validateSQLIdentifier(p.PrimaryKeyColumn, f.Child("primaryKeyColumn")); err != nil {
		all = append(all, err)
	}
	for i, col := range p.IncludeColumns {
		if err := validateSQLIdentifier(col, f.Child("includeColumns").Index(i)); err != nil {
			all = append(all, err)
		}
	}
	for i, col := range p.ExcludeColumns {
		if err := validateSQLIdentifier(col, f.Child("excludeColumns").Index(i)); err != nil {
			all = append(all, err)
		}
	}
	if p.HeartbeatIntervalSeconds != nil && *p.HeartbeatIntervalSeconds < 0 {
		all = append(all, field.Invalid(f.Child("heartbeatIntervalSeconds"), *p.HeartbeatIntervalSeconds, "must be >= 0"))
	}
	return all
}

func validatePostgreSQLTableRef(table string, f *field.Path) *field.Error {
	table = strings.TrimSpace(table)
	if table == "" {
		return field.Required(f, "table reference is required")
	}
	if !postgresTableRefRe.MatchString(table) {
		return field.Invalid(f, table, "must be schema.table or table (letters, digits, underscore)")
	}
	return nil
}

var sqlIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateSQLIdentifier(col string, f *field.Path) *field.Error {
	if col == "" {
		return nil
	}
	if !sqlIdentifierRe.MatchString(col) {
		return field.Invalid(f, col,
			"must be a valid SQL identifier (letters, digits, underscore; must not start with a digit)")
	}
	return nil
}

func validateOrderByColumn(col string, f *field.Path) *field.Error {
	return validateSQLIdentifier(col, f)
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
	if err := validateOrderByColumn(t.OrderByColumn, f.Child("orderByColumn")); err != nil {
		all = append(all, err)
	}
	if err := validateSQLIdentifier(t.ChangeTrackingColumn, f.Child("changeTrackingColumn")); err != nil {
		all = append(all, err)
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
	if n.IncrementalBySnapshot != nil && *n.IncrementalBySnapshot {
		if strings.TrimSpace(n.Query) != "" {
			all = append(all, field.Invalid(f.Child("query"), n.Query,
				"query is not supported when incrementalBySnapshot is true"))
		}
	}
	if id := strings.TrimSpace(n.StartSnapshotID); id != "" {
		if _, err := parseNessieSnapshotID(id); err != nil {
			all = append(all, field.Invalid(f.Child("startSnapshotID"), id, err.Error()))
		}
	}
	return all
}

func parseNessieSnapshotID(s string) (int64, error) {
	u, err := strconv.ParseUint(s, 10, 63)
	if err != nil {
		return 0, fmt.Errorf("must be a non-negative integer snapshot ID: %w", err)
	}
	return int64(u), nil
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
	all = append(all, validateFlattenMetadataSpec(n.RawMode, n.FlattenMetadataColumns, n.FlattenMetadataColumnsPrefix, f)...)
	return all
}

func validateIcebergSource(i *IcebergSourceSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasCatalogURI := i.CatalogURI != "" || i.CatalogURISecretRef != nil
	if !hasCatalogURI {
		all = append(all, field.Required(f.Child("catalogURI"), "catalogURI or catalogURISecretRef is required"))
	}
	hasNamespace := i.Namespace != "" || i.NamespaceSecretRef != nil
	if !hasNamespace {
		all = append(all, field.Required(f.Child("namespace"), "namespace or namespaceSecretRef is required"))
	}
	hasTable := i.Table != "" || i.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if i.CatalogURISecretRef != nil {
		all = append(all, validateSecretRef(i.CatalogURISecretRef, f.Child("catalogURISecretRef"))...)
	}
	if i.NamespaceSecretRef != nil {
		all = append(all, validateSecretRef(i.NamespaceSecretRef, f.Child("namespaceSecretRef"))...)
	}
	if i.TableSecretRef != nil {
		all = append(all, validateSecretRef(i.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	if i.TokenSecretRef != nil {
		all = append(all, validateSecretRef(i.TokenSecretRef, f.Child("tokenSecretRef"))...)
	}
	if i.OAuth2ServerURISecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ServerURISecretRef, f.Child("oauth2ServerURISecretRef"))...)
	}
	if i.OAuth2ClientIDSecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ClientIDSecretRef, f.Child("oauth2ClientIDSecretRef"))...)
	}
	if i.OAuth2ClientSecretSecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ClientSecretSecretRef, f.Child("oauth2ClientSecretSecretRef"))...)
	}
	all = append(all, validateIcebergRESTAuthConfig(string(i.AuthenticationType), i.BearerToken, i.TokenSecretRef, i.BasicAuth, i.OAuth2ClientID, i.OAuth2ClientIDSecretRef, i.OAuth2ClientSecret, i.OAuth2ClientSecretSecretRef, f)...)
	if i.IncrementalBySnapshot != nil && *i.IncrementalBySnapshot {
		if strings.TrimSpace(i.Query) != "" {
			all = append(all, field.Invalid(f.Child("query"), i.Query,
				"query is not supported when incrementalBySnapshot is true"))
		}
	}
	if id := strings.TrimSpace(i.StartSnapshotID); id != "" {
		if _, err := parseNessieSnapshotID(id); err != nil {
			all = append(all, field.Invalid(f.Child("startSnapshotID"), id, err.Error()))
		}
	}
	return all
}

func validateIcebergSink(i *IcebergSinkSpec, f *field.Path) field.ErrorList {
	var all field.ErrorList
	hasCatalogURI := i.CatalogURI != "" || i.CatalogURISecretRef != nil
	if !hasCatalogURI {
		all = append(all, field.Required(f.Child("catalogURI"), "catalogURI or catalogURISecretRef is required"))
	}
	hasNamespace := i.Namespace != "" || i.NamespaceSecretRef != nil
	if !hasNamespace {
		all = append(all, field.Required(f.Child("namespace"), "namespace or namespaceSecretRef is required"))
	}
	hasTable := i.Table != "" || i.TableSecretRef != nil
	if !hasTable {
		all = append(all, field.Required(f.Child("table"), "table or tableSecretRef is required"))
	}
	if i.CatalogURISecretRef != nil {
		all = append(all, validateSecretRef(i.CatalogURISecretRef, f.Child("catalogURISecretRef"))...)
	}
	if i.NamespaceSecretRef != nil {
		all = append(all, validateSecretRef(i.NamespaceSecretRef, f.Child("namespaceSecretRef"))...)
	}
	if i.TableSecretRef != nil {
		all = append(all, validateSecretRef(i.TableSecretRef, f.Child("tableSecretRef"))...)
	}
	if i.TokenSecretRef != nil {
		all = append(all, validateSecretRef(i.TokenSecretRef, f.Child("tokenSecretRef"))...)
	}
	if i.OAuth2ServerURISecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ServerURISecretRef, f.Child("oauth2ServerURISecretRef"))...)
	}
	if i.OAuth2ClientIDSecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ClientIDSecretRef, f.Child("oauth2ClientIDSecretRef"))...)
	}
	if i.OAuth2ClientSecretSecretRef != nil {
		all = append(all, validateSecretRef(i.OAuth2ClientSecretSecretRef, f.Child("oauth2ClientSecretSecretRef"))...)
	}
	hasAK := i.AccessKeySecretRef != nil
	hasSK := i.SecretAccessKeySecretRef != nil
	if hasAK != hasSK {
		all = append(all, field.Invalid(f.Child("accessKeySecretRef"), i.AccessKeySecretRef, "accessKeySecretRef and secretAccessKeySecretRef must both be set or both omitted"))
	}
	if i.AccessKeySecretRef != nil {
		all = append(all, validateSecretRef(i.AccessKeySecretRef, f.Child("accessKeySecretRef"))...)
	}
	if i.SecretAccessKeySecretRef != nil {
		all = append(all, validateSecretRef(i.SecretAccessKeySecretRef, f.Child("secretAccessKeySecretRef"))...)
	}
	all = append(all, validateIcebergRESTAuthConfig(string(i.AuthenticationType), i.BearerToken, i.TokenSecretRef, i.BasicAuth, i.OAuth2ClientID, i.OAuth2ClientIDSecretRef, i.OAuth2ClientSecret, i.OAuth2ClientSecretSecretRef, f)...)
	all = append(all, validateFlattenMetadataSpec(i.RawMode, i.FlattenMetadataColumns, i.FlattenMetadataColumnsPrefix, f)...)
	return all
}

func validateIcebergRESTAuthConfig(authType, bearerToken string, tokenSecretRef *SecretRef, basicAuth *BasicAuthConfig, oauth2ClientID string, oauth2ClientIDSecretRef *SecretRef, oauth2ClientSecret string, oauth2ClientSecretSecretRef *SecretRef, f *field.Path) field.ErrorList {
	all := validateNessieAuthConfig(authType, bearerToken, tokenSecretRef, basicAuth, f)
	hasOAuthID := oauth2ClientID != "" || oauth2ClientIDSecretRef != nil
	hasOAuthSecret := oauth2ClientSecret != "" || oauth2ClientSecretSecretRef != nil
	if hasOAuthID != hasOAuthSecret {
		all = append(all, field.Invalid(f.Child("oauth2ClientID"), oauth2ClientID, "oauth2ClientID and oauth2ClientSecret must both be set or both omitted"))
	}
	hasBearer := bearerToken != "" || tokenSecretRef != nil
	if hasBearer && (hasOAuthID || hasOAuthSecret) {
		all = append(all, field.Invalid(f.Child("bearerToken"), bearerToken, "bearerToken/tokenSecretRef cannot be combined with oauth2ClientID/oauth2ClientSecret"))
	}
	return all
}

func validateFlattenMetadataSpec(rawMode, flatten *bool, prefix string, f *field.Path) field.ErrorList {
	if flatten == nil || !*flatten {
		return nil
	}
	var all field.ErrorList
	if rawMode == nil || !*rawMode {
		all = append(all, field.Invalid(f.Child("flattenMetadataColumns"), *flatten,
			"flattenMetadataColumns requires rawMode to be true"))
	}
	if prefix != "" {
		for i, r := range prefix {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
				all = append(all, field.Invalid(f.Child("flattenMetadataColumnsPrefix"), prefix,
					"flattenMetadataColumnsPrefix may only contain letters, digits, and underscores"))
				break
			}
			if i == 0 && r >= '0' && r <= '9' {
				all = append(all, field.Invalid(f.Child("flattenMetadataColumnsPrefix"), prefix,
					"flattenMetadataColumnsPrefix must not start with a digit"))
				break
			}
		}
	}
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
	all = append(all, validateKafkaSecurityProtocol(k.SecurityProtocol, k.TLS, k.SASL, f.Child("securityProtocol"))...)
	return all
}

func validateKafkaSecurityProtocol(protocol string, tls *TLSConfig, sasl *SASLConfig, f *field.Path) field.ErrorList {
	if protocol == "" {
		return nil
	}
	var all field.ErrorList
	normalized, err := normalizeKafkaSecurityProtocol(protocol)
	if err != nil {
		all = append(all, field.NotSupported(f, protocol, []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}))
		return all
	}
	hasTLS := tls != nil
	hasSASL := kafkaSASLConfigured(sasl)

	switch normalized {
	case "PLAINTEXT":
		if hasSASL {
			all = append(all, field.Forbidden(f, fmt.Sprintf("%s cannot be used with sasl configuration", protocol)))
		}
		if hasTLS {
			all = append(all, field.Forbidden(f, fmt.Sprintf("%s cannot be used with tls configuration", protocol)))
		}
	case "SSL":
		if hasSASL {
			all = append(all, field.Forbidden(f, fmt.Sprintf("%s cannot be used with sasl configuration", protocol)))
		}
		if !hasTLS {
			all = append(all, field.Required(f, "tls configuration is required for securityProtocol SSL"))
		}
	case "SASL_PLAINTEXT":
		if hasTLS {
			all = append(all, field.Forbidden(f, fmt.Sprintf("%s cannot be used with tls configuration", protocol)))
		}
		if !hasSASL {
			all = append(all, field.Required(f, "sasl configuration is required for securityProtocol SASL_PLAINTEXT"))
		}
	case "SASL_SSL":
		if !hasTLS {
			all = append(all, field.Required(f, "tls configuration is required for securityProtocol SASL_SSL"))
		}
		if !hasSASL {
			all = append(all, field.Required(f, "sasl configuration is required for securityProtocol SASL_SSL"))
		}
	}
	return all
}

func normalizeKafkaSecurityProtocol(protocol string) (string, error) {
	if protocol == "" {
		return "", nil
	}
	normalized := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(protocol, "-", "_"), " ", "_"))
	switch normalized {
	case "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported security protocol: %s", protocol)
	}
}

func kafkaSASLConfigured(sasl *SASLConfig) bool {
	if sasl == nil {
		return false
	}
	hasUser := sasl.Username != "" || sasl.UsernameSecretRef != nil
	hasPass := sasl.Password != "" || sasl.PasswordSecretRef != nil
	return hasUser && hasPass
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
	if p.UpsertMode != nil && *p.UpsertMode {
		if err := validateSQLIdentifier(resolveConflictKeyForValidation(p.ConflictKey), f.Child("conflictKey")); err != nil {
			all = append(all, err)
		}
	}
	if p.UpsertStrategy != nil && *p.UpsertStrategy == "ifNewer" {
		if p.UpsertVersionColumn == nil || strings.TrimSpace(*p.UpsertVersionColumn) == "" {
			all = append(all, field.Required(f.Child("upsertVersionColumn"), "upsertVersionColumn is required when upsertStrategy is ifNewer"))
		} else if err := validateSQLIdentifier(*p.UpsertVersionColumn, f.Child("upsertVersionColumn")); err != nil {
			all = append(all, err)
		}
	}
	if p.UpsertVersionColumn != nil && strings.TrimSpace(*p.UpsertVersionColumn) != "" {
		if err := validateSQLIdentifier(*p.UpsertVersionColumn, f.Child("upsertVersionColumn")); err != nil {
			all = append(all, err)
		}
	}
	if p.UpsertStrategy != nil && *p.UpsertStrategy != "" && *p.UpsertStrategy != "always" && *p.UpsertStrategy != "ifNewer" {
		all = append(all, field.Invalid(f.Child("upsertStrategy"), *p.UpsertStrategy, "must be always or ifNewer"))
	}
	all = append(all, validateFlattenMetadataSpec(p.RawMode, p.FlattenMetadataColumns, p.FlattenMetadataColumnsPrefix, f)...)
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
	if t.UpsertMode != nil && *t.UpsertMode {
		conflictKey := resolveConflictKeyForValidation(t.ConflictKey)
		if conflictKey == "" {
			all = append(all, field.Required(f.Child("conflictKey"), "conflictKey is required when upsertMode is enabled"))
		} else if err := validateSQLIdentifier(conflictKey, f.Child("conflictKey")); err != nil {
			all = append(all, err)
		}
		catalog := strings.ToLower(t.Catalog)
		if catalog != "" && !strings.Contains(catalog, "iceberg") {
			all = append(all, field.Invalid(f.Child("catalog"), t.Catalog,
				"upsertMode requires an Iceberg catalog (catalog name should contain \"iceberg\")"))
		}
	}
	all = append(all, validateFlattenMetadataSpec(t.RawMode, t.FlattenMetadataColumns, t.FlattenMetadataColumnsPrefix, f)...)
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
	if err := validateOrderByColumn(c.OrderByColumn, f.Child("orderByColumn")); err != nil {
		all = append(all, err)
	}
	if err := validateSQLIdentifier(c.ChangeTrackingColumn, f.Child("changeTrackingColumn")); err != nil {
		all = append(all, err)
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
	if c.UpsertMode != nil && *c.UpsertMode {
		if c.ConflictKey != nil && strings.TrimSpace(*c.ConflictKey) != "" {
			if err := validateSQLIdentifier(*c.ConflictKey, f.Child("conflictKey")); err != nil {
				all = append(all, err)
			}
		}
	}
	if c.TableEngine != nil && *c.TableEngine != "" && *c.TableEngine != "MergeTree" && *c.TableEngine != "ReplacingMergeTree" {
		all = append(all, field.Invalid(f.Child("tableEngine"), *c.TableEngine, "must be MergeTree or ReplacingMergeTree"))
	}
	if c.ReplacingVersionColumn != nil && strings.TrimSpace(*c.ReplacingVersionColumn) != "" {
		if err := validateSQLIdentifier(*c.ReplacingVersionColumn, f.Child("replacingVersionColumn")); err != nil {
			all = append(all, err)
		}
	}
	all = append(all, validateFlattenMetadataSpec(c.RawMode, c.FlattenMetadataColumns, c.FlattenMetadataColumnsPrefix, f)...)
	return all
}

func resolveConflictKeyForValidation(conflictKey *string) string {
	if conflictKey == nil {
		return ""
	}
	return strings.TrimSpace(*conflictKey)
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
		if !transformtypes.IsRegistered(t.Type) {
			all = append(all, field.NotSupported(idx.Child("type"), t.Type, transformtypes.All()))
			continue
		}
		hasConfig := t.Config != nil && len(t.Config.Raw) > 0
		switch t.Type {
		case transformtypes.Timestamp:
			if hasConfig {
				var cfg TimestampTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid timestamp config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "timestamp transformation configuration is required"))
			}
		case transformtypes.Flatten:
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
		case transformtypes.Filter:
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
		case transformtypes.Mask:
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
		case transformtypes.Router:
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
		case transformtypes.Select:
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
		case transformtypes.Remove:
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
		case transformtypes.SnakeCase:
			if hasConfig {
				var cfg SnakeCaseTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid snakeCase config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "snakeCase transformation configuration is required"))
			}
		case transformtypes.CamelCase:
			if hasConfig {
				var cfg CamelCaseTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid camelCase config: "+err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "camelCase transformation configuration is required"))
			}
		case transformtypes.DebeziumUnwrap:
			if hasConfig {
				var cfg DebeziumUnwrapTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid debeziumUnwrap config: "+err.Error()))
				} else if cfg.SnapshotOperation != "" && cfg.SnapshotOperation != "insert" && cfg.SnapshotOperation != "update" {
					all = append(all, field.NotSupported(idx.Child("config", "snapshotOperation"), cfg.SnapshotOperation, []string{"insert", "update"}))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "debeziumUnwrap transformation configuration is required"))
			}
		case transformtypes.ReplaceField:
			if hasConfig {
				var cfg ReplaceFieldTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid replaceField config: "+err.Error()))
				} else {
					if len(cfg.Include) > 0 && len(cfg.Exclude) > 0 {
						all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "include and exclude are mutually exclusive"))
					}
					if len(cfg.Renames) == 0 && len(cfg.Include) == 0 && len(cfg.Exclude) == 0 {
						all = append(all, field.Required(idx.Child("config"), "at least one of renames, include, or exclude is required"))
					}
					for j, rename := range cfg.Renames {
						if !isValidColonMapping(rename) {
							all = append(all, field.Invalid(idx.Child("config", "renames").Index(j), rename, "rename must be in oldPath:newPath format"))
						}
					}
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "replaceField transformation configuration is required"))
			}
		case transformtypes.HeadersToPayload:
			if hasConfig {
				var cfg HeadersToPayloadTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid headersToPayload config: "+err.Error()))
				} else if len(cfg.Mappings) == 0 {
					all = append(all, field.Required(idx.Child("config", "mappings"), "at least one mapping is required"))
				} else {
					for j, mapping := range cfg.Mappings {
						if !isValidColonMapping(mapping) {
							all = append(all, field.Invalid(idx.Child("config", "mappings").Index(j), mapping, "mapping must be in headerName:fieldPath format"))
						}
					}
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "headersToPayload transformation configuration is required"))
			}
		case transformtypes.StructFlatten:
			if hasConfig {
				var cfg StructFlattenTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid structFlatten config: "+err.Error()))
				} else if err := validateStructFlattenDelimiter(t.Config.Raw); err != nil {
					all = append(all, field.Invalid(idx.Child("config", "delimiter"), cfg.Delimiter, err.Error()))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "structFlatten transformation configuration is required"))
			}
		case transformtypes.ExtractField:
			if hasConfig {
				var cfg ExtractFieldTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid extractField config: "+err.Error()))
				} else if normalizeJSONPathField(cfg.Field) == "" {
					all = append(all, field.Required(idx.Child("config", "field"), "field is required"))
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "extractField transformation configuration is required"))
			}
		case transformtypes.HoistField:
			if hasConfig {
				var cfg HoistFieldTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid hoistField config: "+err.Error()))
				} else {
					fieldName := strings.TrimSpace(cfg.Field)
					if fieldName == "" {
						all = append(all, field.Required(idx.Child("config", "field"), "field is required"))
					} else if strings.Contains(fieldName, ".") {
						all = append(all, field.Invalid(idx.Child("config", "field"), cfg.Field, "field must be a simple top-level key without dots"))
					}
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "hoistField transformation configuration is required"))
			}
		case transformtypes.Cast:
			if hasConfig {
				var cfg CastTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid cast config: "+err.Error()))
				} else if len(cfg.Spec) == 0 {
					all = append(all, field.Required(idx.Child("config", "spec"), "spec is required and must be non-empty"))
				} else {
					for path, typ := range cfg.Spec {
						if normalizeJSONPathField(path) == "" {
							all = append(all, field.Invalid(idx.Child("config", "spec"), path, "spec keys must be non-empty JSONPaths"))
							continue
						}
						if !isValidCastType(typ) {
							all = append(all, field.NotSupported(idx.Child("config", "spec").Key(path), typ, validCastTypes))
						}
					}
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "cast transformation configuration is required"))
			}
		case transformtypes.Timezone:
			if hasConfig {
				var cfg TimezoneTransformation
				if err := json.Unmarshal(t.Config.Raw, &cfg); err != nil {
					all = append(all, field.Invalid(idx.Child("config"), string(t.Config.Raw), "invalid timezone config: "+err.Error()))
				} else {
					if strings.TrimSpace(cfg.Timezone) == "" {
						all = append(all, field.Required(idx.Child("config", "timezone"), "timezone is required"))
					} else if _, err := LoadTimezoneLocation(cfg.Timezone); err != nil {
						all = append(all, field.Invalid(idx.Child("config", "timezone"), cfg.Timezone, "must be a valid IANA timezone or ±HH:MM offset"))
					}
					if len(cfg.Fields) == 0 {
						all = append(all, field.Required(idx.Child("config", "fields"), "fields is required and must be non-empty"))
					} else {
						for i, f := range cfg.Fields {
							if normalizeJSONPathField(f) == "" {
								all = append(all, field.Invalid(idx.Child("config", "fields").Index(i), f, "field path must be a non-empty JSONPath"))
							}
						}
					}
					if src := strings.TrimSpace(cfg.SourceTimezone); src != "" {
						if _, err := LoadTimezoneLocation(src); err != nil {
							all = append(all, field.Invalid(idx.Child("config", "sourceTimezone"), cfg.SourceTimezone, "must be a valid IANA timezone or ±HH:MM offset"))
						}
					}
					if format := strings.TrimSpace(cfg.Format); format != "" && !isValidTimezoneFormat(format) {
						all = append(all, field.NotSupported(idx.Child("config", "format"), cfg.Format, validTimezoneFormats))
					}
				}
			} else {
				all = append(all, field.Required(idx.Child("config"), "timezone transformation configuration is required"))
			}
		}
	}
	return all
}

// validateStructFlattenDelimiter rejects an explicitly empty delimiter while allowing omitted delimiter (default ".").
func validateStructFlattenDelimiter(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil
	}
	delimRaw, ok := fields["delimiter"]
	if !ok {
		return nil
	}
	var delim string
	if err := json.Unmarshal(delimRaw, &delim); err != nil {
		return fmt.Errorf("delimiter must be a string")
	}
	if strings.TrimSpace(delim) == "" {
		return fmt.Errorf("delimiter must be a non-empty string")
	}
	return nil
}

// normalizeJSONPathField strips a leading $. or $ prefix (same as runtime normalizeFieldPath).
func normalizeJSONPathField(field string) string {
	field = strings.TrimSpace(field)
	switch {
	case strings.HasPrefix(field, "$."):
		return field[2:]
	case strings.HasPrefix(field, "$"):
		return field[1:]
	default:
		return field
	}
}

var validCastTypes = []string{"string", "int64", "float64", "bool", "null"}

func isValidCastType(typ string) bool {
	for _, allowed := range validCastTypes {
		if typ == allowed {
			return true
		}
	}
	return false
}

var validTimezoneFormats = []string{"RFC3339", "RFC3339Nano", "UnixMilli"}

func isValidTimezoneFormat(format string) bool {
	for _, allowed := range validTimezoneFormats {
		if format == allowed {
			return true
		}
	}
	return false
}

// isValidColonMapping reports whether s is in left:right form with non-empty sides.
func isValidColonMapping(s string) bool {
	left, right, ok := strings.Cut(s, ":")
	if !ok {
		return false
	}
	return strings.TrimSpace(left) != "" && strings.TrimSpace(right) != ""
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
