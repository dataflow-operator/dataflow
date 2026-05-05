package runtimeimage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dataflow-operator/dataflow/internal/version"
)

func TestProcessorImage(t *testing.T) {
	t.Run("env unset uses default", func(t *testing.T) {
		t.Setenv(ProcessorImageEnv, "")
		assert.Equal(t, version.DefaultProcessorImage(), ProcessorImage())
	})

	t.Run("env set overrides", func(t *testing.T) {
		t.Setenv(ProcessorImageEnv, "registry.example/proc:custom")
		assert.Equal(t, "registry.example/proc:custom", ProcessorImage())
	})
}
