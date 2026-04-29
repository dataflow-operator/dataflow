package providers

import (
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

type SourceConfigValidator func(raw []byte, path *field.Path) field.ErrorList
type SinkConfigValidator func(raw []byte, path *field.Path) field.ErrorList

type SourceDefinition struct {
	Type               string
	SupportsCheckpoint bool
	ValidateConfig     SourceConfigValidator
}

type SinkDefinition struct {
	Type           string
	ValidateConfig SinkConfigValidator
}

var (
	mu      sync.RWMutex
	sources = map[string]SourceDefinition{}
	sinks   = map[string]SinkDefinition{}
)

func RegisterSource(def SourceDefinition) {
	if def.Type == "" {
		panic("providers: source type is required")
	}
	mu.Lock()
	defer mu.Unlock()
	existing := sources[def.Type]
	existing.Type = def.Type
	existing.SupportsCheckpoint = existing.SupportsCheckpoint || def.SupportsCheckpoint
	if def.ValidateConfig != nil {
		existing.ValidateConfig = def.ValidateConfig
	}
	sources[def.Type] = existing
}

func RegisterSink(def SinkDefinition) {
	if def.Type == "" {
		panic("providers: sink type is required")
	}
	mu.Lock()
	defer mu.Unlock()
	existing := sinks[def.Type]
	existing.Type = def.Type
	if def.ValidateConfig != nil {
		existing.ValidateConfig = def.ValidateConfig
	}
	sinks[def.Type] = existing
}

func ListSourceTypes() []string {
	mu.RLock()
	defer mu.RUnlock()
	types := make([]string, 0, len(sources))
	for t := range sources {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

func ListSinkTypes() []string {
	mu.RLock()
	defer mu.RUnlock()
	types := make([]string, 0, len(sinks))
	for t := range sinks {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

func SourceSupportsCheckpoint(sourceType string) bool {
	mu.RLock()
	defer mu.RUnlock()
	def, ok := sources[sourceType]
	return ok && def.SupportsCheckpoint
}

func SourceValidator(sourceType string) SourceConfigValidator {
	mu.RLock()
	defer mu.RUnlock()
	return sources[sourceType].ValidateConfig
}

func SinkValidator(sinkType string) SinkConfigValidator {
	mu.RLock()
	defer mu.RUnlock()
	return sinks[sinkType].ValidateConfig
}
