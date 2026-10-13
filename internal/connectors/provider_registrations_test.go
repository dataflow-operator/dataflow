package connectors

import (
	"testing"

	"github.com/dataflow-operator/dataflow/internal/providers"
)

func TestProviderFactoryCoverage(t *testing.T) {
	if err := providerFactoryCoverage(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisteredSourcesExposeCheckpointAndExhausts(t *testing.T) {
	if !providers.SourceSupportsCheckpoint("postgresql-cdc") {
		t.Fatal("postgresql-cdc must keep checkpoint support")
	}
	if providers.SourceExhausts("postgresql-cdc") {
		t.Fatal("postgresql-cdc checkpoints but does not finish a one-shot read")
	}
	if providers.SourceSupportsCheckpoint("kafka") || providers.SourceExhausts("kafka") {
		t.Fatal("kafka must not checkpoint or exhaust")
	}
	if !providers.SourceExhausts("hive") || !providers.SourceSupportsCheckpoint("hive") {
		t.Fatal("hive must checkpoint and exhaust")
	}
}
