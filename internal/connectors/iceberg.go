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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// IcebergSourceConnector implements SourceConnector for Apache Iceberg REST catalog.
type IcebergSourceConnector struct {
	baseConnectorRWMutex
	connectorLogger
	connectorMetadata
	config                    *v1.IcebergSourceSpec
	cat                       *rest.Catalog
	tbl                       *table.Table
	channelBufferSize         int
	checkpointStore           checkpoint.Store
	sourceType                string
	checkpointMu              sync.Mutex
	lastAckedSnapshotID       int64
	lastAckedSnapshotSequence int64
}

// NewIcebergSourceConnector creates a new Iceberg REST source connector.
func NewIcebergSourceConnector(config *v1.IcebergSourceSpec) *IcebergSourceConnector {
	return NewIcebergSourceConnectorWithOptions(config, nil)
}

// NewIcebergSourceConnectorWithOptions creates a new Iceberg REST source connector with optional settings.
func NewIcebergSourceConnectorWithOptions(config *v1.IcebergSourceSpec, opts *SourceConnectorOptions) *IcebergSourceConnector {
	c := &IcebergSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "iceberg", connectorRole: "source"},
	}
	if opts != nil {
		if icebergIncrementalEnabled(config) {
			c.checkpointStore = opts.CheckpointStore
			c.sourceType = opts.SourceType
			if c.sourceType == "" {
				c.sourceType = "iceberg"
			}
			if len(opts.InitialCheckpoint) > 0 {
				c.applyInitialCheckpoint(opts.InitialCheckpoint)
			}
		}
		if opts.ChannelBufferSize > 0 {
			c.channelBufferSize = opts.ChannelBufferSize
		} else {
			c.channelBufferSize = constants.DefaultChannelBufferSize
		}
	} else {
		c.channelBufferSize = constants.DefaultChannelBufferSize
	}
	return c
}

// Connect establishes connection to the Iceberg REST catalog and loads the table.
func (c *IcebergSourceConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	auth := icebergRESTAuthFromSource(c.config)
	c.logger.Info("Connecting to Iceberg REST catalog",
		"catalogURI", c.config.CatalogURI,
		"namespace", c.config.Namespace,
		"table", c.config.Table)
	if err := runIcebergRESTPreflight(ctx, c.config.CatalogURI, c.config.Warehouse, auth); err != nil {
		return err
	}

	cat, err := newIcebergRESTCatalog(ctx, "iceberg", c.config.CatalogURI, c.config.Warehouse, c.config.Prefix, auth)
	if err != nil {
		return err
	}

	ident := catalog.ToIdentifier(c.config.Namespace, c.config.Table)
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", c.config.Namespace, c.config.Table, err)
	}

	c.cat = cat
	c.tbl = tbl
	c.logger.Info("Successfully connected to Iceberg REST catalog", "namespace", c.config.Namespace, "table", c.config.Table)
	return nil
}

// Read returns a channel of messages from the Iceberg table (polling).
func (c *IcebergSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if c.tbl == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}
	c.logger.Info("Starting to read from Iceberg", "namespace", c.config.Namespace, "table", c.config.Table)
	pollInterval := 10 * time.Second
	if c.config.PollInterval != nil && *c.config.PollInterval > 0 {
		pollInterval = time.Duration(*c.config.PollInterval) * time.Second
	}
	return runPollingRead(ctx, pollInterval, c.readOnce, c.channelBufferSize, &pollingReadOpts{
		logger: c.logger,
		meta:   &c.connectorMetadata,
	}), nil
}

func (c *IcebergSourceConnector) readOnce(ctx context.Context, msgChan chan *types.Message) error {
	c.RLock()
	closed := c.Closed()
	tbl := c.tbl
	c.RUnlock()
	if closed || tbl == nil {
		return nil
	}

	if err := tbl.Refresh(ctx); err != nil {
		c.RecordError("read", "refresh_error")
		return fmt.Errorf("iceberg refresh: %w", err)
	}

	if icebergIncrementalEnabled(c.config) {
		return c.readOnceIncremental(ctx, msgChan, tbl)
	}
	return c.readOnceFullScan(ctx, msgChan, tbl)
}

func (c *IcebergSourceConnector) readOnceFullScan(ctx context.Context, msgChan chan *types.Message, tbl *table.Table) error {
	pollStart := time.Now()
	arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		c.RecordError("read", "scan_error")
		return fmt.Errorf("iceberg scan: %w", err)
	}
	defer arrowTbl.Release()

	msgs := arrowTableToMessages(arrowTbl, c.config.Namespace, c.config.Table, false)
	if len(msgs) == 0 {
		return ErrSourceExhausted
	}
	for _, msg := range msgs {
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.logger.Info("Iceberg poll cycle completed",
		"namespace", c.config.Namespace,
		"table", c.config.Table,
		"mode", "full_scan",
		"rows_total", len(msgs),
		"duration_ms", time.Since(pollStart).Milliseconds(),
	)
	return nil
}

func (c *IcebergSourceConnector) readOnceIncremental(ctx context.Context, msgChan chan *types.Message, tbl *table.Table) error {
	pollStart := time.Now()
	current := tbl.CurrentSnapshot()
	if current == nil {
		return ErrSourceExhausted
	}

	c.checkpointMu.Lock()
	afterID := c.lastAckedSnapshotID
	afterSeq := c.lastAckedSnapshotSequence
	c.checkpointMu.Unlock()

	hasAfter := afterSeq > 0 || afterID != 0

	var afterPtr *int64
	if hasAfter {
		afterPtr = &afterID
	} else if start := strings.TrimSpace(c.config.StartSnapshotID); start != "" {
		if id, err := parseSnapshotIDString(start); err == nil {
			afterPtr = &id
		}
	}

	lookup := tbl.SnapshotByID
	chain, foundAfter := buildSnapshotChain(current, lookup, afterPtr)
	if !hasAfter && afterPtr == nil {
		chain = []table.Snapshot{*current}
		foundAfter = true
	}
	if !foundAfter && hasAfter {
		c.logger.Info("Last acked snapshot not found in table lineage; resetting read position",
			"lastAckedSnapshotID", afterID,
			"namespace", c.config.Namespace,
			"table", c.config.Table)
		if start := strings.TrimSpace(c.config.StartSnapshotID); start != "" {
			if id, err := parseSnapshotIDString(start); err == nil {
				afterPtr = &id
				chain, _ = buildSnapshotChain(current, lookup, afterPtr)
			}
		} else {
			chain, _ = buildSnapshotChain(current, lookup, nil)
			if len(chain) > 0 {
				chain = chain[len(chain)-1:]
			}
		}
	}

	if len(chain) == 0 {
		return ErrSourceExhausted
	}

	var total int
	for _, snap := range chain {
		arrowTbl, err := tbl.Scan(table.WithSnapshotID(snap.SnapshotID)).ToArrowTable(ctx)
		if err != nil {
			c.RecordError("read", "scan_error")
			return fmt.Errorf("iceberg scan snapshot %d: %w", snap.SnapshotID, err)
		}
		msgs := arrowTableToMessages(arrowTbl, c.config.Namespace, c.config.Table, false)
		arrowTbl.Release()
		if len(msgs) == 0 {
			continue
		}
		snapID := snap.SnapshotID
		snapSeq := snap.SequenceNumber
		for _, msg := range msgs {
			if msg.Metadata == nil {
				msg.Metadata = make(map[string]interface{})
			}
			msg.Metadata["snapshot_id"] = snapID
			msg.Metadata["snapshot_sequence"] = snapSeq
			msg.Ack = func() { c.advanceCheckpoint(snapID, snapSeq) }
			select {
			case msgChan <- msg:
				total++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if total == 0 {
		return ErrSourceExhausted
	}
	lastSnap := chain[len(chain)-1]
	c.logger.Info("Iceberg poll cycle completed",
		"namespace", c.config.Namespace,
		"table", c.config.Table,
		"mode", "incremental",
		"snapshots_read", len(chain),
		"rows_total", total,
		"duration_ms", time.Since(pollStart).Milliseconds(),
		"from_snapshot_id", afterID,
		"to_snapshot_id", lastSnap.SnapshotID,
		"from_snapshot_sequence", afterSeq,
		"to_snapshot_sequence", lastSnap.SequenceNumber,
	)
	return nil
}

// Close closes the Iceberg source connector.
func (c *IcebergSourceConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Iceberg source connection", "table", c.config.Table)
	c.tbl = nil
	c.cat = nil
	return nil
}

// IcebergSinkConnector implements SinkConnector for Apache Iceberg REST catalog.
type IcebergSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	progressRecorder
	rawModeConfig
	flattenMetadataSinkState
	config          *v1.IcebergSinkSpec
	cat             *rest.Catalog
	tbl             *table.Table
	metaColumnTypes map[string]iceberg.Type
}

// NewIcebergSinkConnector creates a new Iceberg REST sink connector.
func NewIcebergSinkConnector(config *v1.IcebergSinkSpec) *IcebergSinkConnector {
	return &IcebergSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "iceberg", connectorRole: "sink"},
		rawModeConfig: rawModeConfig{
			RawMode:                      config.RawMode,
			FlattenMetadataColumns:       config.FlattenMetadataColumns,
			FlattenMetadataColumnsPrefix: config.FlattenMetadataColumnsPrefix,
		},
	}
}

// Connect establishes connection to the Iceberg REST catalog and loads or creates the table.
func (c *IcebergSinkConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	auth := icebergRESTAuthFromSink(c.config)
	c.logger.Info("Connecting to Iceberg REST sink",
		"catalogURI", c.config.CatalogURI,
		"namespace", c.config.Namespace,
		"table", c.config.Table)
	if err := runIcebergRESTPreflight(ctx, c.config.CatalogURI, c.config.Warehouse, auth); err != nil {
		return err
	}

	cat, err := newIcebergRESTCatalog(ctx, "iceberg", c.config.CatalogURI, c.config.Warehouse, c.config.Prefix, auth)
	if err != nil {
		return err
	}

	ident := catalog.ToIdentifier(c.config.Namespace, c.config.Table)
	var tbl *table.Table
	exists, err := cat.CheckTableExists(ctx, ident)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		tbl, err = cat.LoadTable(ctx, ident)
		if err != nil {
			return fmt.Errorf("failed to load table: %w", err)
		}
		if c.rawMode() {
			if c.flattenMetadataColumns() {
				metaCols, err := nessieFlattenMetaColumnNamesFromTable(tbl)
				if err != nil {
					return fmt.Errorf("table %s.%s: %w", c.config.Namespace, c.config.Table, err)
				}
				c.metaColumnNames = metaCols
				c.metaColumnTypes = icebergRESTMetaColumnTypesFromTable(tbl, metaCols)
			} else if err := validateIcebergRESTRawModeSchema(tbl); err != nil {
				return fmt.Errorf("table %s.%s: %w", c.config.Namespace, c.config.Table, err)
			}
		}
	} else if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
		if c.rawMode() && c.flattenMetadataColumns() {
			c.deferredTableCreate = true
			c.logger.Info("Deferring Iceberg table creation until first batch with metadata keys",
				"namespace", c.config.Namespace, "table", c.config.Table)
		} else {
			schema := icebergRESTDefaultSchema(c.rawMode())
			tbl, err = cat.CreateTable(ctx, ident, schema)
			if err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
			c.logger.Info("Created Iceberg table", "namespace", c.config.Namespace, "table", c.config.Table, "rawMode", c.rawMode())
		}
	} else {
		return fmt.Errorf("table %s.%s does not exist and AutoCreateTable is not set", c.config.Namespace, c.config.Table)
	}

	c.cat = cat
	c.tbl = tbl
	c.logger.Info("Successfully connected to Iceberg REST sink", "namespace", c.config.Namespace, "table", c.config.Table)
	return nil
}

// Write writes messages to the Iceberg table via REST catalog.
func (c *IcebergSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if c.cat == nil {
		return fmt.Errorf("not connected, call Connect first")
	}
	if c.tbl == nil && !c.deferredTableCreate {
		return fmt.Errorf("not connected, call Connect first")
	}

	cfg := ApplyAckGranularity(NewBatchWriteConfig(c.config.BatchSize, c.config.BatchFlushIntervalSeconds, 100), c.ackGranularityIsMessage())
	return RunBatchWriteLoop(ctx, messages, cfg, BatchWriteOptions{
		Logger:    c.logger,
		LogFields: []any{"table", c.config.Table},
		OnFlush:   c.flushBatch,
		OnAck: func(msgs []*types.Message) {
			c.AckAfterSuccessfulWrite(msgs)
		},
	})
}

func (c *IcebergSinkConnector) flushBatch(batchCtx context.Context, msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := c.ensureFlattenMetadataTable(batchCtx, msgs); err != nil {
		return err
	}
	arrowTbl, err := c.buildArrowTableFromMessages(msgs)
	if err != nil {
		return err
	}
	defer arrowTbl.Release()

	appendSize := int64(100)
	if c.config.BatchSize != nil && *c.config.BatchSize > 0 {
		appendSize = int64(*c.config.BatchSize)
	}
	if appendSize == 0 {
		appendSize = int64(len(msgs))
	}

	return retry.OnRetry(batchCtx, retry.NessieAppendMaxAttempts, retry.NessieAppendInitialBackoff, func(err error) bool {
		return isRetryableIcebergRESTAppendError(err)
	}, func() error {
		attemptCtx, cancel := BatchWriteContext(batchCtx)
		defer cancel()
		newTbl, appendErr := c.tbl.AppendTable(attemptCtx, arrowTbl, appendSize, nil)
		if appendErr != nil {
			if isRetryableIcebergRESTSnapshotConflict(appendErr) {
				if refreshErr := c.tbl.Refresh(attemptCtx); refreshErr != nil {
					return fmt.Errorf("append table: %w (refresh after snapshot conflict: %v)", appendErr, refreshErr)
				}
			}
			return fmt.Errorf("append table: %w", appendErr)
		}
		if newTbl != nil {
			c.tbl = newTbl
		}
		return nil
	})
}

// Close closes the Iceberg sink connector.
func (c *IcebergSinkConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Iceberg sink connection", "table", c.config.Table)
	c.tbl = nil
	c.cat = nil
	c.flattenMetadataSinkState = flattenMetadataSinkState{}
	c.metaColumnTypes = nil
	return nil
}

func (c *IcebergSinkConnector) ensureFlattenMetadataTable(ctx context.Context, msgs []*types.Message) error {
	if !c.flattenMetadataColumns() || c.tbl != nil {
		return nil
	}
	if !c.deferredTableCreate {
		return fmt.Errorf("flattenMetadataColumns: table is not loaded")
	}
	cols, err := collectFlattenMetadataColumnNames(msgs, c.flattenMetadataPrefix())
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("flattenMetadataColumns: no metadata keys found in first batch")
	}
	colTypes := inferFlattenColumnTypes(msgs, cols, c.flattenMetadataPrefix())
	schema := nessieIcebergSchemaFlattened(cols, colTypes)
	ident := catalog.ToIdentifier(c.config.Namespace, c.config.Table)
	tbl, err := c.cat.CreateTable(ctx, ident, schema)
	if err != nil {
		return fmt.Errorf("failed to create flattened metadata table: %w", err)
	}
	c.tbl = tbl
	c.metaColumnNames = cols
	c.metaColumnTypes = colTypes
	c.deferredTableCreate = false
	c.logger.Info("Created Iceberg table with flattened metadata columns",
		"namespace", c.config.Namespace,
		"table", c.config.Table,
		"columns", cols)
	return nil
}

func (c *IcebergSinkConnector) buildArrowTableFromMessages(msgs []*types.Message) (arrow.Table, error) {
	if c.flattenMetadataColumns() {
		if len(c.metaColumnNames) == 0 {
			return nil, fmt.Errorf("flattenMetadataColumns: metadata columns not initialized")
		}
		colTypes := c.metaColumnTypes
		if colTypes == nil {
			colTypes = inferFlattenColumnTypes(msgs, c.metaColumnNames, c.flattenMetadataPrefix())
		}
		return messagesToArrowTableFlattened(msgs, c.metaColumnNames, colTypes, c.flattenMetadataPrefix(), c.logger)
	}
	return messagesToArrowTable(msgs, c.rawMode())
}
