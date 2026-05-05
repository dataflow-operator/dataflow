package k8snames

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessorChildNames(t *testing.T) {
	const name = "my-flow"
	assert.Equal(t, "df-my-flow", ProcessorDeployment(name))
	assert.Equal(t, "df-my-flow-spec", ProcessorSpecConfigMap(name))
	assert.Equal(t, "df-my-flow-checkpoint", ProcessorCheckpointConfigMap(name))
	assert.Equal(t, "df-my-flow-processor", ProcessorServiceAccount(name))
}
