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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/dataflow-operator/dataflow/pkg/sinkbatch"
	"github.com/go-logr/logr"
)

var openHiveMetastore = func(ctx context.Context, uri string) (hiveMetastore, error) {
	return dialThriftHiveMetastore(ctx, uri)
}

var newHiveFileSystem = func(cfg hiveStorageConfig) hiveFileSystem {
	return newRemoteHiveFS(cfg)
}

type hiveCursor struct {
	Partition string `json:"partition,omitempty"`
	File      string `json:"file,omitempty"`
	RowOffset int64  `json:"rowOffset"`
	Database  string `json:"database,omitempty"`
	Table     string `json:"table,omitempty"`
}

func hiveCursorLess(part, file string, row int64, other hiveCursor) bool {
	if part != other.Partition {
		return part < other.Partition
	}
	if file != other.File {
		return file < other.File
	}
	return row < other.RowOffset
}

func hiveCursorBefore(a, b hiveCursor) bool {
	return hiveCursorLess(a.Partition, a.File, a.RowOffset, b)
}

type hiveScanTarget struct {
	Key      string
	Location string
	Values   []string
}

// HiveSourceConnector reads Parquet files described by the Hive Metastore.
type HiveSourceConnector struct {
	baseConnectorRWMutex
	connectorLogger
	connectorMetadata
	config            *v1.HiveSourceSpec
	hms               hiveMetastore
	fs                hiveFileSystem
	table             hiveTableMeta
	channelBufferSize int
	checkpointStore   checkpoint.Store
	sourceType        string
	checkpointMu      sync.Mutex
	readResume        hiveCursor
	persisted         hiveCursor
}

// NewHiveSourceConnector creates a Hive source connector.
func NewHiveSourceConnector(config *v1.HiveSourceSpec) *HiveSourceConnector {
	return NewHiveSourceConnectorWithOptions(config, nil)
}

// NewHiveSourceConnectorWithOptions creates a Hive source connector with checkpoint options.
func NewHiveSourceConnectorWithOptions(config *v1.HiveSourceSpec, opts *SourceConnectorOptions) *HiveSourceConnector {
	c := &HiveSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "hive", connectorRole: "source"},
		channelBufferSize: constants.DefaultChannelBufferSize,
		sourceType:        "hive",
	}
	if opts != nil {
		c.checkpointStore = opts.CheckpointStore
		if opts.SourceType != "" {
			c.sourceType = opts.SourceType
		}
		if opts.ChannelBufferSize > 0 {
			c.channelBufferSize = opts.ChannelBufferSize
		}
		if len(opts.InitialCheckpoint) > 0 {
			c.applyInitialCheckpoint(opts.InitialCheckpoint)
		}
	}
	return c
}

func (c *HiveSourceConnector) applyInitialCheckpoint(data []byte) {
	var cp hiveCursor
	if err := json.Unmarshal(data, &cp); err != nil {
		return
	}
	if c.config != nil && cp.Database != "" && cp.Table != "" {
		if cp.Database != c.config.Database || cp.Table != c.config.Table {
			return
		}
	}
	c.checkpointMu.Lock()
	c.readResume = cp
	c.persisted = cp
	c.checkpointMu.Unlock()
}

// Connect loads the Hive table and checks that it is Parquet.
func (c *HiveSourceConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()
	if c.config == nil {
		return fmt.Errorf("hive source configuration is required")
	}
	c.logger.Info("Connecting to Hive Metastore", "metastoreURI", c.config.MetastoreURI, "database", c.config.Database, "table", c.config.Table)
	hms, err := openHiveMetastore(ctx, c.config.MetastoreURI)
	if err != nil {
		return err
	}
	table, err := hms.GetTable(ctx, c.config.Database, c.config.Table)
	if err != nil {
		_ = hms.Close()
		return fmt.Errorf("load Hive table %s.%s: %w", c.config.Database, c.config.Table, err)
	}
	if err := hiveFormatSupported(table); err != nil {
		_ = hms.Close()
		return err
	}
	for _, col := range table.Columns {
		if !supportedHiveType(col.Type) {
			_ = hms.Close()
			return fmt.Errorf("unsupported Hive column %s type %q", col.Name, col.Type)
		}
	}
	c.hms = hms
	c.table = table
	c.fs = newHiveFileSystem(hiveStorageFromEndpoint(c.config.HiveEndpointSpec))
	return nil
}

// Read polls the table until a poll finds no rows past the checkpoint.
func (c *HiveSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if c.hms == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}
	pollInterval := 10 * time.Second
	if c.config.PollInterval != nil && *c.config.PollInterval > 0 {
		pollInterval = time.Duration(*c.config.PollInterval) * time.Second
	}
	return runPollingRead(ctx, pollInterval, c.readOnce, c.channelBufferSize, &pollingReadOpts{
		logger: c.logger,
		meta:   &c.connectorMetadata,
	}), nil
}

func (c *HiveSourceConnector) readOnce(ctx context.Context, msgChan chan *types.Message) error {
	c.RLock()
	closed := c.Closed()
	c.RUnlock()
	if closed || c.hms == nil {
		return nil
	}
	targets, err := c.scanTargets(ctx)
	if err != nil {
		c.RecordError("read", "metastore_error")
		return err
	}
	c.checkpointMu.Lock()
	resume := c.readResume
	c.checkpointMu.Unlock()

	limits := lakehousePollLimitsFrom(c.config.MaxRowsPerPoll, c.config.MaxBytesPerPoll)
	emitted := 0
	var bytes int64
	for _, target := range targets {
		files, err := c.fs.ListParquet(ctx, target.Location)
		if err != nil {
			c.RecordError("read", "list_error")
			return err
		}
		sort.Strings(files)
		for _, file := range files {
			data, err := c.fs.Read(ctx, file)
			if err != nil {
				c.RecordError("read", "read_error")
				return err
			}
			rows, err := decodeParquetRows(data)
			if err != nil {
				c.RecordError("read", "parquet_error")
				return err
			}
			for i, row := range rows {
				if hiveCursorLess(target.Key, file, int64(i), resume) {
					continue
				}
				for pi, key := range c.table.PartitionKeys {
					if pi < len(target.Values) {
						if _, exists := row[key.Name]; !exists {
							row[key.Name] = target.Values[pi]
						}
					}
				}
				payload, err := json.Marshal(row)
				if err != nil {
					return err
				}
				if limits.active() && emitted > 0 && ((limits.maxRows > 0 && int64(emitted) >= limits.maxRows) || (limits.maxBytes > 0 && bytes >= limits.maxBytes)) {
					return nil
				}
				msg, err := types.FromJSON(row)
				if err != nil {
					msg = types.NewMessage(payload)
				}
				if msg.Metadata == nil {
					msg.Metadata = map[string]interface{}{}
				}
				msg.Metadata["database"] = c.config.Database
				msg.Metadata["table"] = c.config.Table
				msg.Metadata["file"] = file
				if target.Key != "" {
					msg.Metadata["partition"] = target.Key
				}
				next := hiveCursor{
					Partition: target.Key,
					File:      file,
					RowOffset: int64(i) + 1,
					Database:  c.config.Database,
					Table:     c.config.Table,
				}
				msg.Ack = func() { c.noteAck(next) }
				select {
				case msgChan <- msg:
				case <-ctx.Done():
					return ctx.Err()
				}
				c.checkpointMu.Lock()
				c.readResume = next
				resume = next
				c.checkpointMu.Unlock()
				emitted++
				bytes += int64(len(msg.Data))
				if limits.maxRows > 0 && int64(emitted) >= limits.maxRows {
					return nil
				}
				if limits.maxBytes > 0 && bytes >= limits.maxBytes {
					return nil
				}
			}
		}
	}
	if emitted == 0 {
		return ErrSourceExhausted
	}
	return nil
}

func (c *HiveSourceConnector) scanTargets(ctx context.Context) ([]hiveScanTarget, error) {
	if len(c.table.PartitionKeys) == 0 {
		return []hiveScanTarget{{Location: c.table.Location}}, nil
	}
	parts, err := c.hms.GetPartitions(ctx, c.config.Database, c.config.Table)
	if err != nil {
		return nil, err
	}
	targets := make([]hiveScanTarget, 0, len(parts))
	for _, part := range parts {
		key, err := hivePartitionPath(c.table.PartitionKeys, part.Values)
		if err != nil {
			return nil, err
		}
		loc := part.Location
		if loc == "" {
			base, err := parseHiveLocation(c.table.Location)
			if err != nil {
				return nil, err
			}
			loc, err = base.child(key)
			if err != nil {
				return nil, err
			}
		}
		targets = append(targets, hiveScanTarget{Key: key, Location: loc, Values: append([]string(nil), part.Values...)})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Key < targets[j].Key })
	return targets, nil
}

func (c *HiveSourceConnector) noteAck(next hiveCursor) {
	c.checkpointMu.Lock()
	if !hiveCursorBefore(c.persisted, next) {
		c.checkpointMu.Unlock()
		return
	}
	c.persisted = next
	data, err := json.Marshal(next)
	store := c.checkpointStore
	sourceType := c.sourceType
	c.checkpointMu.Unlock()
	if err != nil || store == nil {
		return
	}
	saveErr := store.Save(context.Background(), sourceType, data)
	reportCheckpointSaveError(c.logger, &c.connectorMetadata, sourceType, saveErr)
}

// Close closes the metastore client and file-system clients.
func (c *HiveSourceConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Hive source", "table", c.config.Table)
	if c.hms != nil {
		_ = c.hms.Close()
		c.hms = nil
	}
	if c.fs != nil {
		_ = c.fs.Close()
		c.fs = nil
	}
	return nil
}

// HiveSinkConnector appends Parquet files and registers partitions in the Hive Metastore.
type HiveSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	progressRecorder
	rawModeConfig
	config *v1.HiveSinkSpec
	hms    hiveMetastore
	fs     hiveFileSystem
	table  hiveTableMeta
}

// NewHiveSinkConnector creates a Hive sink connector.
func NewHiveSinkConnector(config *v1.HiveSinkSpec) *HiveSinkConnector {
	raw := false
	if config != nil && config.RawMode != nil {
		raw = *config.RawMode
	}
	return &HiveSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "hive", connectorRole: "sink"},
		rawModeConfig:     rawModeConfig{RawMode: &raw},
	}
}

// Connect loads or creates the Hive table.
func (c *HiveSinkConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()
	if c.config == nil {
		return fmt.Errorf("hive sink configuration is required")
	}
	c.logger.Info("Connecting to Hive Metastore sink", "metastoreURI", c.config.MetastoreURI, "database", c.config.Database, "table", c.config.Table)
	hms, err := openHiveMetastore(ctx, c.config.MetastoreURI)
	if err != nil {
		return err
	}
	table, err := hms.GetTable(ctx, c.config.Database, c.config.Table)
	if err != nil {
		if !isHiveNotFound(err) || c.config.AutoCreateTable == nil || !*c.config.AutoCreateTable {
			_ = hms.Close()
			return fmt.Errorf("load Hive table %s.%s: %w", c.config.Database, c.config.Table, err)
		}
		created := hiveTableMeta{
			DB:           c.config.Database,
			Name:         c.config.Table,
			Location:     c.config.Location,
			InputFormat:  hiveParquetInputFormat,
			OutputFormat: hiveParquetOutputFormat,
			SerDe:        hiveParquetSerDe,
			Columns:      hiveRawColumns(),
			TableType:    "EXTERNAL_TABLE",
			Parameters:   map[string]string{"EXTERNAL": "TRUE"},
		}
		if cerr := hms.CreateTable(ctx, created); cerr != nil && !isHiveAlreadyExists(cerr) {
			_ = hms.Close()
			return fmt.Errorf("create Hive table: %w", cerr)
		}
		table, err = hms.GetTable(ctx, c.config.Database, c.config.Table)
		if err != nil {
			_ = hms.Close()
			return fmt.Errorf("reload Hive table: %w", err)
		}
	}
	if err := hiveFormatSupported(table); err != nil {
		_ = hms.Close()
		return err
	}
	if c.rawMode() {
		if !hiveTableIsRaw(table.Columns) {
			_ = hms.Close()
			return fmt.Errorf("rawMode requires Hive columns data and _metadata")
		}
	} else {
		for _, col := range table.Columns {
			if !supportedHiveType(col.Type) {
				_ = hms.Close()
				return fmt.Errorf("unsupported Hive column %s type %q", col.Name, col.Type)
			}
		}
	}
	c.hms = hms
	c.table = table
	c.fs = newHiveFileSystem(hiveStorageFromEndpoint(c.config.HiveEndpointSpec))
	return nil
}

// Write appends batches as Parquet files.
func (c *HiveSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if c.hms == nil {
		return fmt.Errorf("not connected, call Connect first")
	}
	cfg := ApplyAckGranularity(NewBatchWriteConfig(c.config.BatchSize, c.config.BatchFlushIntervalSeconds, int(sinkbatch.DefaultHiveBatchSize)), c.shouldCollapseBatchForAck())
	return RunBatchWriteLoop(ctx, messages, cfg, BatchWriteOptions{
		Logger:    c.logger,
		LogFields: []any{"table", c.config.Table},
		OnFlush:   c.flushBatch,
		OnAck: func(msgs []*types.Message) {
			c.AckAfterSuccessfulWrite(msgs)
		},
	})
}

func (c *HiveSinkConnector) flushBatch(ctx context.Context, msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	table, err := c.hms.GetTable(ctx, c.config.Database, c.config.Table)
	if err != nil {
		return err
	}
	c.table = table
	groups, err := c.groupByPartition(msgs)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	locations, err := c.partitionLocations(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		batch := groups[key]
		loc := c.table.Location
		if key != "" {
			if existing, ok := locations[key]; ok && existing != "" {
				loc = existing
			} else {
				base, err := parseHiveLocation(c.table.Location)
				if err != nil {
					return err
				}
				loc, err = base.child(key)
				if err != nil {
					return err
				}
			}
		}
		base, err := parseHiveLocation(loc)
		if err != nil {
			return err
		}
		fileURL, err := base.child(hiveBatchObjectName(batch))
		if err != nil {
			return err
		}
		rows, cols, err := c.rowsForBatch(batch)
		if err != nil {
			return err
		}
		payload, err := encodeParquet(cols, rows)
		if err != nil {
			return err
		}
		if err := c.fs.Write(ctx, fileURL, payload); err != nil {
			return fmt.Errorf("write %s: %w", fileURL, err)
		}
		if key != "" {
			if _, ok := locations[key]; !ok {
				values := batchValues(c.table.PartitionKeys, batch[0])
				part := hivePartitionMeta{
					DB:       c.config.Database,
					Table:    c.config.Table,
					Values:   values,
					Location: loc,
				}
				if err := c.hms.AddPartition(ctx, part); err != nil && !isHiveAlreadyExists(err) {
					return fmt.Errorf("add partition %s: %w", key, err)
				}
				locations[key] = loc
			}
		}
	}
	return nil
}

func (c *HiveSinkConnector) groupByPartition(msgs []*types.Message) (map[string][]*types.Message, error) {
	groups := map[string][]*types.Message{}
	if len(c.table.PartitionKeys) == 0 {
		groups[""] = msgs
		return groups, nil
	}
	for _, msg := range msgs {
		values, err := messagePartitionValues(msg, c.table.PartitionKeys)
		if err != nil {
			return nil, err
		}
		key, err := hivePartitionPath(c.table.PartitionKeys, values)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], msg)
	}
	return groups, nil
}

func (c *HiveSinkConnector) partitionLocations(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	if len(c.table.PartitionKeys) == 0 {
		return out, nil
	}
	parts, err := c.hms.GetPartitions(ctx, c.config.Database, c.config.Table)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		key, err := hivePartitionPath(c.table.PartitionKeys, part.Values)
		if err != nil {
			return nil, err
		}
		out[key] = part.Location
	}
	return out, nil
}

func (c *HiveSinkConnector) rowsForBatch(msgs []*types.Message) ([]map[string]interface{}, []hiveField, error) {
	if c.rawMode() {
		cols := hiveRawColumns()
		rows := make([]map[string]interface{}, 0, len(msgs))
		for _, msg := range msgs {
			data, meta := extractDataAndMetadata(msg)
			rows = append(rows, map[string]interface{}{"data": data, "_metadata": meta})
		}
		return rows, cols, nil
	}
	cols := append([]hiveField(nil), c.table.Columns...)
	rows := make([]map[string]interface{}, 0, len(msgs))
	for _, msg := range msgs {
		obj, err := msg.ToJSON()
		if err != nil {
			return nil, nil, fmt.Errorf("hive sink expects a JSON object: %w", err)
		}
		obj = unwrapMessageDataForColumns(obj)
		row := make(map[string]interface{}, len(cols))
		for _, col := range cols {
			row[col.Name] = obj[col.Name]
		}
		rows = append(rows, row)
	}
	return rows, cols, nil
}

// Close closes the Hive sink.
func (c *HiveSinkConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Hive sink", "table", c.config.Table)
	if c.hms != nil {
		_ = c.hms.Close()
		c.hms = nil
	}
	if c.fs != nil {
		_ = c.fs.Close()
		c.fs = nil
	}
	return nil
}

func hiveStorageFromEndpoint(spec v1.HiveEndpointSpec) hiveStorageConfig {
	return hiveStorageConfig{
		Endpoint:       firstNonEmpty(spec.S3Endpoint, os.Getenv("AWS_S3_ENDPOINT")),
		Region:         firstNonEmpty(spec.S3Region, os.Getenv("AWS_REGION")),
		AccessKey:      firstNonEmpty(spec.AccessKey, os.Getenv("AWS_ACCESS_KEY_ID")),
		SecretKey:      firstNonEmpty(spec.SecretKey, os.Getenv("AWS_SECRET_ACCESS_KEY")),
		ForcePathStyle: spec.ForcePathStyle,
		HDFSUser:       spec.HDFSUser,
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func hiveBatchObjectName(msgs []*types.Message) string {
	h := sha256.New()
	for _, msg := range msgs {
		id := types.MessageID(msg)
		if id == "" {
			sum := sha256.Sum256(msg.Data)
			id = hex.EncodeToString(sum[:8])
		}
		_, _ = h.Write([]byte(id))
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return "dataflow-" + hex.EncodeToString(sum[:16]) + ".parquet"
}

func messagePartitionValues(msg *types.Message, keys []hiveField) ([]string, error) {
	obj, err := msg.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("hive sink expects a JSON object: %w", err)
	}
	obj = unwrapMessageDataForColumns(obj)
	return objectPartitionValues(obj, keys)
}

func batchValues(keys []hiveField, msg *types.Message) []string {
	values, err := messagePartitionValues(msg, keys)
	if err != nil {
		return nil
	}
	return values
}

func objectPartitionValues(obj map[string]interface{}, keys []hiveField) ([]string, error) {
	values := make([]string, len(keys))
	for i, key := range keys {
		v, ok := obj[key.Name]
		if !ok || v == nil {
			return nil, fmt.Errorf("message is missing partition column %s", key.Name)
		}
		values[i] = strings.Trim(asHiveString(v), `"`)
	}
	return values, nil
}

func isHiveNotFound(err error) bool {
	return errors.Is(err, errHiveNotFound)
}

func isHiveAlreadyExists(err error) bool {
	return errors.Is(err, errHiveAlreadyExists)
}
