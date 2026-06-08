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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

const nessieSnapshotConflictToken = "snapshot id changed"

// buildNessieIcebergURI returns the Iceberg REST catalog URI for Nessie.
// Format: {baseURL}/iceberg[/{branch}][|{warehouse}]
func buildNessieIcebergURI(baseURL, branch, warehouse string) string {
	baseURL = strings.TrimSuffix(baseURL, "/")
	path := baseURL + "/iceberg"
	if branch != "" {
		path += "/" + branch
	}
	if warehouse != "" {
		path += "|" + warehouse
	}
	return path
}

func resolveNessieAuthentication(authType v1.NessieAuthenticationType, bearerToken string, basicAuth *v1.BasicAuthConfig) (token string, basic string) {
	mode := strings.ToUpper(strings.TrimSpace(string(authType)))
	if mode == "" {
		mode = string(v1.NessieAuthenticationAuto)
	}
	switch mode {
	case string(v1.NessieAuthenticationNone):
		return "", ""
	case string(v1.NessieAuthenticationBearer):
		return bearerToken, ""
	case string(v1.NessieAuthenticationBasic):
		if basicAuth != nil && basicAuth.Username != "" && basicAuth.Password != "" {
			return "", "Basic " + base64.StdEncoding.EncodeToString([]byte(basicAuth.Username+":"+basicAuth.Password))
		}
		return "", ""
	case string(v1.NessieAuthenticationAuto):
		fallthrough
	default:
		if bearerToken != "" {
			return bearerToken, ""
		}
		if basicAuth != nil && basicAuth.Username != "" && basicAuth.Password != "" {
			return "", "Basic " + base64.StdEncoding.EncodeToString([]byte(basicAuth.Username+":"+basicAuth.Password))
		}
		return "", ""
	}
}

func nessieAuthOptions(authType v1.NessieAuthenticationType, bearerToken string, basicAuth *v1.BasicAuthConfig) []rest.Option {
	var opts []rest.Option
	token, basic := resolveNessieAuthentication(authType, bearerToken, basicAuth)
	if token != "" {
		opts = append(opts, rest.WithOAuthToken(token))
	}
	if basic != "" {
		opts = append(opts, rest.WithCustomTransport(&basicAuthTransport{base: http.DefaultTransport, auth: basic}))
	}
	return opts
}

const (
	nessiePreflightTimeout = 5 * time.Second
	maxPreflightBodyBytes  = 4096
)

type nessiePreflightConfig struct {
	baseURL     string
	branch      string
	authType    v1.NessieAuthenticationType
	bearerToken string
	basicAuth   *v1.BasicAuthConfig
}

func (c *NessieSourceConnector) preflightConfig() nessiePreflightConfig {
	return nessiePreflightConfig{
		baseURL:     c.config.BaseURL,
		branch:      c.config.Branch,
		authType:    c.config.AuthenticationType,
		bearerToken: c.config.BearerToken,
		basicAuth:   c.config.BasicAuth,
	}
}

func (c *NessieSinkConnector) preflightConfig() nessiePreflightConfig {
	return nessiePreflightConfig{
		baseURL:     c.config.BaseURL,
		branch:      c.config.Branch,
		authType:    c.config.AuthenticationType,
		bearerToken: c.config.BearerToken,
		basicAuth:   c.config.BasicAuth,
	}
}

func runNessiePreflight(ctx context.Context, cfg nessiePreflightConfig) error {
	baseURL := strings.TrimSuffix(strings.TrimSpace(cfg.baseURL), "/")
	if baseURL == "" {
		return fmt.Errorf("nessie preflight: baseURL is empty")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("nessie preflight: invalid baseURL %q: %w", cfg.baseURL, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("nessie preflight: baseURL must include scheme and host, got %q", cfg.baseURL)
	}

	preflightCtx, cancel := context.WithTimeout(ctx, nessiePreflightTimeout)
	defer cancel()
	client := &http.Client{}

	if err := nessiePreflightRequest(preflightCtx, client, baseURL+"/api/v2/config", cfg, "server config"); err != nil {
		return err
	}

	branch := cfg.branch
	if branch == "" {
		branch = "main"
	}
	refURL := fmt.Sprintf("%s/api/v2/trees/%s", baseURL, url.PathEscape(branch))
	if err := nessiePreflightRequest(preflightCtx, client, refURL, cfg, fmt.Sprintf("branch %q", branch)); err != nil {
		return err
	}

	return nil
}

func nessiePreflightRequest(ctx context.Context, client *http.Client, endpoint string, cfg nessiePreflightConfig, what string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("nessie preflight: failed to prepare %s request: %w", what, err)
	}
	token, basic := resolveNessieAuthentication(cfg.authType, cfg.bearerToken, cfg.basicAuth)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if basic != "" {
		req.Header.Set("Authorization", basic)
	}

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("nessie preflight: timeout while checking %s at %s", what, endpoint)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("nessie preflight: context canceled while checking %s at %s: %w", what, endpoint, ctx.Err())
		}
		return fmt.Errorf("nessie preflight: failed to reach %s at %s: %w", what, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxPreflightBodyBytes))
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return fmt.Errorf("nessie preflight: %s check failed (%s): %s", what, endpoint, msg)
	}

	return nil
}

// basicAuthTransport adds Authorization header to outgoing requests.
type basicAuthTransport struct {
	base http.RoundTripper
	auth string
}

func (t *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2.Header.Set("Authorization", t.auth)
	if t.base != nil {
		return t.base.RoundTrip(req2)
	}
	return http.DefaultTransport.RoundTrip(req2)
}

// NessieSourceConnector implements SourceConnector for Nessie (Iceberg REST catalog).
type NessieSourceConnector struct {
	baseConnectorRWMutex
	connectorLogger
	connectorMetadata
	config                    *v1.NessieSourceSpec
	cat                       *rest.Catalog
	tbl                       *table.Table
	channelBufferSize         int
	checkpointStore           checkpoint.Store
	sourceType                string
	checkpointMu              sync.Mutex
	lastAckedSnapshotID       int64
	lastAckedSnapshotSequence int64
}

// NewNessieSourceConnector creates a new Nessie source connector.
func NewNessieSourceConnector(config *v1.NessieSourceSpec) *NessieSourceConnector {
	return NewNessieSourceConnectorWithOptions(config, nil)
}

// NewNessieSourceConnectorWithOptions creates a new Nessie source connector with optional settings.
func NewNessieSourceConnectorWithOptions(config *v1.NessieSourceSpec, opts *SourceConnectorOptions) *NessieSourceConnector {
	c := &NessieSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "nessie", connectorRole: "source"},
	}
	if opts != nil {
		if nessieIncrementalEnabled(config) {
			c.checkpointStore = opts.CheckpointStore
			c.sourceType = opts.SourceType
			if c.sourceType == "" {
				c.sourceType = "nessie"
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

// Connect establishes connection to Nessie and loads the Iceberg table.
func (c *NessieSourceConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	branch := c.config.Branch
	if branch == "" {
		branch = "main"
	}
	uri := buildNessieIcebergURI(c.config.BaseURL, branch, c.config.Warehouse)
	c.logger.Info("Connecting to Nessie", "uri", uri, "namespace", c.config.Namespace, "table", c.config.Table)
	if err := runNessiePreflight(ctx, c.preflightConfig()); err != nil {
		return err
	}

	opts := nessieAuthOptions(c.config.AuthenticationType, c.config.BearerToken, c.config.BasicAuth)
	if c.config.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(c.config.Warehouse))
	}

	cat, err := rest.NewCatalog(ctx, "nessie", uri, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Nessie catalog client: %w", err)
	}

	ident := catalog.ToIdentifier(c.config.Namespace, c.config.Table)
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", c.config.Namespace, c.config.Table, err)
	}

	c.cat = cat
	c.tbl = tbl
	c.logger.Info("Successfully connected to Nessie", "namespace", c.config.Namespace, "table", c.config.Table)
	return nil
}

// Read returns a channel of messages from the Iceberg table (polling).
func (c *NessieSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if c.tbl == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}
	c.logger.Info("Starting to read from Nessie", "namespace", c.config.Namespace, "table", c.config.Table)
	pollInterval := 10 * time.Second
	if c.config.PollInterval != nil && *c.config.PollInterval > 0 {
		pollInterval = time.Duration(*c.config.PollInterval) * time.Second
	}
	return runPollingRead(ctx, pollInterval, c.readOnce, c.channelBufferSize, &pollingReadOpts{
		logger: c.logger,
		meta:   &c.connectorMetadata,
	}), nil
}

func (c *NessieSourceConnector) readOnce(ctx context.Context, msgChan chan *types.Message) error {
	c.RLock()
	closed := c.Closed()
	tbl := c.tbl
	c.RUnlock()
	if closed || tbl == nil {
		return nil
	}

	if err := tbl.Refresh(ctx); err != nil {
		c.RecordError("read", "refresh_error")
		return fmt.Errorf("nessie refresh: %w", err)
	}

	if nessieIncrementalEnabled(c.config) {
		return c.readOnceIncremental(ctx, msgChan, tbl)
	}
	return c.readOnceFullScan(ctx, msgChan, tbl)
}

func (c *NessieSourceConnector) readOnceFullScan(ctx context.Context, msgChan chan *types.Message, tbl *table.Table) error {
	pollStart := time.Now()
	arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		c.RecordError("read", "scan_error")
		return fmt.Errorf("nessie scan: %w", err)
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
	c.logger.Info("Nessie poll cycle completed",
		"namespace", c.config.Namespace,
		"table", c.config.Table,
		"mode", "full_scan",
		"rows_total", len(msgs),
		"duration_ms", time.Since(pollStart).Milliseconds(),
	)
	return nil
}

func (c *NessieSourceConnector) readOnceIncremental(ctx context.Context, msgChan chan *types.Message, tbl *table.Table) error {
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
			return fmt.Errorf("nessie scan snapshot %d: %w", snap.SnapshotID, err)
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
	c.logger.Info("Nessie poll cycle completed",
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

// Close closes the Nessie source connector.
func (c *NessieSourceConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Nessie source connection", "table", c.config.Table)
	c.tbl = nil
	c.cat = nil
	return nil
}

// NessieSinkConnector implements SinkConnector for Nessie (Iceberg REST catalog).
type NessieSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	progressRecorder
	rawModeConfig
	flattenMetadataSinkState
	config *v1.NessieSinkSpec
	cat    *rest.Catalog
	tbl    *table.Table
	// metaColumnTypes maps flattened column name to Iceberg type (Nessie-specific).
	metaColumnTypes map[string]iceberg.Type
}

// NewNessieSinkConnector creates a new Nessie sink connector.
func NewNessieSinkConnector(config *v1.NessieSinkSpec) *NessieSinkConnector {
	return &NessieSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "nessie", connectorRole: "sink"},
		rawModeConfig: rawModeConfig{
			RawMode:                      config.RawMode,
			FlattenMetadataColumns:       config.FlattenMetadataColumns,
			FlattenMetadataColumnsPrefix: config.FlattenMetadataColumnsPrefix,
		},
	}
}

func nessieMetaColumnTypesFromTable(tbl *table.Table, metaColumns []string) map[string]iceberg.Type {
	types := make(map[string]iceberg.Type, len(metaColumns))
	if tbl == nil || tbl.Schema() == nil {
		for _, col := range metaColumns {
			types[col] = iceberg.PrimitiveTypes.String
		}
		return types
	}
	schema := tbl.Schema()
	for _, col := range metaColumns {
		if f, ok := schema.FindFieldByName(col); ok {
			types[col] = f.Type
		} else {
			types[col] = iceberg.PrimitiveTypes.String
		}
	}
	return types
}

func nessieIcebergSchema(rawMode bool) *iceberg.Schema {
	if rawMode {
		return iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 2, Name: "_metadata", Type: iceberg.PrimitiveTypes.String, Required: false},
		)
	}
	return iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false})
}

func validateNessieRawModeSchema(tbl *table.Table) error {
	if tbl == nil {
		return fmt.Errorf("table is nil")
	}
	schema := tbl.Schema()
	if schema == nil {
		return fmt.Errorf("table schema is nil")
	}
	if _, ok := schema.FindFieldByNameCaseInsensitive("data"); !ok {
		return fmt.Errorf("rawMode requires a \"data\" column in the Iceberg table")
	}
	if _, ok := schema.FindFieldByNameCaseInsensitive("_metadata"); !ok {
		return fmt.Errorf("rawMode requires a \"_metadata\" column in the Iceberg table")
	}
	return nil
}

// Connect establishes connection to Nessie and loads or creates the Iceberg table.
func (c *NessieSinkConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	branch := c.config.Branch
	if branch == "" {
		branch = "main"
	}
	uri := buildNessieIcebergURI(c.config.BaseURL, branch, c.config.Warehouse)
	c.logger.Info("Connecting to Nessie sink", "uri", uri, "namespace", c.config.Namespace, "table", c.config.Table)
	if err := runNessiePreflight(ctx, c.preflightConfig()); err != nil {
		return err
	}

	opts := nessieAuthOptions(c.config.AuthenticationType, c.config.BearerToken, c.config.BasicAuth)
	if c.config.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(c.config.Warehouse))
	}

	cat, err := rest.NewCatalog(ctx, "nessie", uri, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Nessie catalog client: %w", err)
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
				c.metaColumnTypes = nessieMetaColumnTypesFromTable(tbl, metaCols)
			} else if err := validateNessieRawModeSchema(tbl); err != nil {
				return fmt.Errorf("table %s.%s: %w", c.config.Namespace, c.config.Table, err)
			}
		}
	} else if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
		if c.rawMode() && c.flattenMetadataColumns() {
			c.deferredTableCreate = true
			c.logger.Info("Deferring Iceberg table creation until first batch with metadata keys",
				"namespace", c.config.Namespace, "table", c.config.Table)
		} else {
			schema := nessieIcebergSchema(c.rawMode())
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
	c.logger.Info("Successfully connected to Nessie sink", "namespace", c.config.Namespace, "table", c.config.Table)
	return nil
}

// Write writes messages to the Iceberg table via Nessie.
func (c *NessieSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if c.cat == nil {
		return fmt.Errorf("not connected, call Connect first")
	}
	if c.tbl == nil && !c.deferredTableCreate {
		return fmt.Errorf("not connected, call Connect first")
	}

	cfg := NewBatchWriteConfig(c.config.BatchSize, c.config.BatchFlushIntervalSeconds, 100)
	return RunBatchWriteLoop(ctx, messages, cfg, BatchWriteOptions{
		Logger:    c.logger,
		LogFields: []any{"table", c.config.Table},
		OnFlush:   c.flushBatch,
		OnAck: func(msgs []*types.Message) {
			c.AckMessagesAndNotifyProgress(msgs)
		},
	})
}

func (c *NessieSinkConnector) flushBatch(batchCtx context.Context, msgs []*types.Message) error {
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
		return isRetryableNessieAppendError(err)
	}, func() error {
		attemptCtx, cancel := BatchWriteContext(batchCtx)
		defer cancel()
		newTbl, appendErr := c.tbl.AppendTable(attemptCtx, arrowTbl, appendSize, nil)
		if appendErr != nil {
			if isRetryableNessieSnapshotConflict(appendErr) {
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

func isRetryableNessieSnapshotConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), nessieSnapshotConflictToken)
}

func isRetryableNessieAppendError(err error) bool {
	if err == nil {
		return false
	}
	if retry.IsTimeoutError(err) || retry.IsRetryableTransient(err) || isRetryableNessieSnapshotConflict(err) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, ": eof") || strings.Contains(lower, "context canceled")
}

// Close closes the Nessie sink connector.
func (c *NessieSinkConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()
	c.logger.Info("Closing Nessie sink connection", "table", c.config.Table)
	c.tbl = nil
	c.cat = nil
	c.flattenMetadataSinkState = flattenMetadataSinkState{}
	c.metaColumnTypes = nil
	return nil
}

func (c *NessieSinkConnector) ensureFlattenMetadataTable(ctx context.Context, msgs []*types.Message) error {
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

func (c *NessieSinkConnector) buildArrowTableFromMessages(msgs []*types.Message) (arrow.Table, error) {
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

// arrowTableToMessages converts an Arrow table to types.Message slice.
func arrowTableToMessages(tbl arrow.Table, namespace, tableName string, rawMode bool) []*types.Message {
	if tbl.NumRows() == 0 {
		return nil
	}
	schema := tbl.Schema()
	cols := schema.Fields()
	// Resolve each column to a single arrow.Array (concatenate chunks)
	nCols := int(tbl.NumCols())
	colArrs := make([]arrow.Array, nCols)
	for i := 0; i < nCols; i++ {
		chunked := tbl.Column(i).Data()
		chunks := chunked.Chunks()
		arr, err := array.Concatenate(chunks, memory.DefaultAllocator)
		if err != nil {
			return nil
		}
		defer arr.Release()
		colArrs[i] = arr
	}
	msgs := make([]*types.Message, 0, tbl.NumRows())
	for r := int64(0); r < tbl.NumRows(); r++ {
		rowMap := make(map[string]interface{})
		for i, f := range cols {
			if colArrs[i].Len() <= int(r) {
				continue
			}
			v := valueAt(colArrs[i], int(r))
			rowMap[f.Name] = v
		}
		var jsonData []byte
		var err error
		isFlatten, flattenPrefix, metaCols := detectFlattenMetadataFromArrowFields(cols)
		if isFlatten {
			var value interface{}
			dataVal := rowMap["data"]
			if s, ok := dataVal.(string); ok {
				if uerr := json.Unmarshal([]byte(s), &value); uerr != nil {
					value = dataVal
				}
			} else {
				value = dataVal
			}
			meta := make(map[string]interface{})
			for _, col := range metaCols {
				key := metadataKeyFromColumn(col, flattenPrefix)
				meta[key] = rowMap[col]
			}
			jsonData, err = buildRawModeJSON(value, meta)
			if err != nil {
				continue
			}
			msg := types.NewMessage(jsonData)
			for k, v := range meta {
				msg.Metadata[k] = v
			}
			msg.Metadata["namespace"] = namespace
			msg.Metadata["table"] = tableName
			msgs = append(msgs, msg)
			continue
		}
		if rawMode {
			metadata := map[string]interface{}{"namespace": namespace, "table": tableName}
			jsonData, err = buildRawModeJSON(rowMap, metadata)
		} else {
			jsonData, err = json.Marshal(rowMap)
		}
		if err != nil {
			continue
		}
		msg := types.NewMessage(jsonData)
		msg.Metadata["namespace"] = namespace
		msg.Metadata["table"] = tableName
		msgs = append(msgs, msg)
	}
	return msgs
}

func valueAt(arr arrow.Array, i int) interface{} {
	if arr.IsNull(i) {
		return nil
	}
	switch a := arr.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Int32:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Timestamp:
		if a.IsNull(i) {
			return nil
		}
		tsType, ok := a.DataType().(*arrow.TimestampType)
		if !ok {
			return a.Value(i)
		}
		toTime, err := tsType.GetToTimeFunc()
		if err != nil {
			return a.Value(i)
		}
		return toTime(a.Value(i)).UTC()
	case *array.Binary:
		return a.Value(i)
	case *array.LargeString:
		return a.Value(i)
	default:
		return arr.ValueStr(i)
	}
}

// messagesToArrowTable builds an Arrow table from messages.
// When rawMode is true, writes data and _metadata columns using extractDataAndMetadata.
func messagesToArrowTable(msgs []*types.Message, rawMode bool) (arrow.Table, error) {
	mem := memory.DefaultAllocator
	if rawMode {
		dataBuilder := array.NewStringBuilder(mem)
		metaBuilder := array.NewStringBuilder(mem)
		defer dataBuilder.Release()
		defer metaBuilder.Release()
		for _, m := range msgs {
			dataStr, metaStr := extractDataAndMetadata(m)
			dataBuilder.Append(dataStr)
			metaBuilder.Append(metaStr)
		}
		dataArr := dataBuilder.NewArray()
		metaArr := metaBuilder.NewArray()
		defer dataArr.Release()
		defer metaArr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "data", Type: arrow.BinaryTypes.String},
			{Name: "_metadata", Type: arrow.BinaryTypes.String},
		}, nil)
		rec := array.NewRecord(schema, []arrow.Array{dataArr, metaArr}, int64(len(msgs)))
		defer rec.Release()
		return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
	}

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for _, m := range msgs {
		builder.Append(string(m.Data))
	}
	arr := builder.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "data", Type: arrow.BinaryTypes.String}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(len(msgs)))
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}
