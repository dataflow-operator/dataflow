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

package connectors

import (
	"github.com/go-logr/logr"
)

const (
	checkpointOpSave  = "checkpoint_save"
	checkpointOpFlush = "checkpoint_flush"
)

// checkpointSaveReporter logs and records metrics for checkpoint persistence failures.
type checkpointSaveReporter struct {
	logger     logr.Logger
	meta       *connectorMetadata
	sourceType string // optional override when meta.connectorType is empty
}

func (r checkpointSaveReporter) report(err error, operation string) {
	if err == nil {
		return
	}
	connectorType := r.sourceType
	if r.meta != nil && r.meta.connectorType != "" {
		connectorType = r.meta.connectorType
	}
	if r.logger.GetSink() != nil {
		r.logger.Error(err, "checkpoint persistence failed", "operation", operation, "connectorType", connectorType)
	}
	if r.meta != nil && r.meta.hasMetadata() {
		r.meta.RecordError(operation, "persistence_error")
	}
}

func reportCheckpointSaveError(logger logr.Logger, meta *connectorMetadata, sourceType string, err error) {
	checkpointSaveReporter{logger: logger, meta: meta, sourceType: sourceType}.report(err, checkpointOpSave)
}

// WireCheckpointSaveReporting configures checkpoint persistence error reporting on supported connectors.
// Call after SetMetadata so metrics include namespace and name.
func WireCheckpointSaveReporting(connector interface{}, logger logr.Logger) {
	switch c := connector.(type) {
	case *PostgreSQLSourceConnector:
		c.cp.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *TrinoSourceConnector:
		c.cp.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *ClickHouseSourceConnector:
		c.cp.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *PostgreSQLCDCSourceConnector:
		c.cp.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata, sourceType: c.sourceType})
	case *PostgreSQLSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *TrinoSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *ClickHouseSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *KafkaSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *IcebergSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	case *NessieSinkConnector:
		c.progressRecorder.setReporter(checkpointSaveReporter{logger: logger, meta: &c.connectorMetadata})
	}
}
