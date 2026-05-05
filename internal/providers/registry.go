// Package providers is a compatibility shim for code that still imports
// internal/providers. New code should use github.com/dataflow-operator/dataflow/pkg/providers.
package providers

import pkg "github.com/dataflow-operator/dataflow/pkg/providers"

type (
	SourceConfigValidator = pkg.SourceConfigValidator
	SinkConfigValidator   = pkg.SinkConfigValidator
	SourceDefinition      = pkg.SourceDefinition
	SinkDefinition        = pkg.SinkDefinition
)

var (
	RegisterSource           = pkg.RegisterSource
	RegisterSink             = pkg.RegisterSink
	ListSourceTypes          = pkg.ListSourceTypes
	ListSinkTypes            = pkg.ListSinkTypes
	SourceSupportsCheckpoint = pkg.SourceSupportsCheckpoint
	SourceValidator          = pkg.SourceValidator
	SinkValidator            = pkg.SinkValidator
)
