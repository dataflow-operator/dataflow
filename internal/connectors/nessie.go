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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
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
	config            *v1.NessieSourceSpec
	cat               *rest.Catalog
	tbl               *table.Table
	channelBufferSize int
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
	if opts != nil && opts.ChannelBufferSize > 0 {
		c.channelBufferSize = opts.ChannelBufferSize
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

	arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		c.RecordError("read", "scan_error")
		return fmt.Errorf("nessie scan: %w", err)
	}
	defer arrowTbl.Release()

	msgs := arrowTableToMessages(arrowTbl, c.config.Namespace, c.config.Table, false)
	for _, msg := range msgs {
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
	config *v1.NessieSinkSpec
	cat    *rest.Catalog
	tbl    *table.Table
}

// NewNessieSinkConnector creates a new Nessie sink connector.
func NewNessieSinkConnector(config *v1.NessieSinkSpec) *NessieSinkConnector {
	return &NessieSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "nessie", connectorRole: "sink"},
	}
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
	} else if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
		schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false})
		tbl, err = cat.CreateTable(ctx, ident, schema)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		c.logger.Info("Created Iceberg table", "namespace", c.config.Namespace, "table", c.config.Table)
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
	if c.tbl == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	batchSize := int64(100)
	if c.config.BatchSize != nil && *c.config.BatchSize > 0 {
		batchSize = int64(*c.config.BatchSize)
	}
	maxBatchSize := int(batchSize)
	if batchSize == 0 {
		maxBatchSize = constants.MaxBatchSizeWhenTimerOnly
	}

	flushIntervalSec := 10
	if c.config.BatchFlushIntervalSeconds != nil {
		flushIntervalSec = int(*c.config.BatchFlushIntervalSeconds)
	}
	useTimer := flushIntervalSec > 0
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	var batch []*types.Message
	var flushTimer *time.Timer

	stopTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
		}
	}

	flush := func(msgs []*types.Message) error {
		if len(msgs) == 0 {
			return nil
		}
		arrowTbl, err := messagesToArrowTable(msgs)
		if err != nil {
			return err
		}
		defer arrowTbl.Release()
		appendSize := batchSize
		if appendSize == 0 {
			appendSize = int64(len(msgs))
		}
		err = retry.OnRetry(ctx, retry.NessieAppendMaxAttempts, retry.NessieAppendInitialBackoff, func(err error) bool {
			return isRetryableNessieAppendError(err)
		}, func() error {
			newTbl, appendErr := c.tbl.AppendTable(ctx, arrowTbl, appendSize, nil)
			if appendErr != nil {
				if isRetryableNessieSnapshotConflict(appendErr) {
					if refreshErr := c.tbl.Refresh(ctx); refreshErr != nil {
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
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if m.Ack != nil {
				m.Ack()
			}
		}
		return nil
	}

	doFlush := func(toFlush []*types.Message) error {
		stopTimer()
		return flush(toFlush)
	}

	for {
		if useTimer && len(batch) > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(flushInterval)
		}

		if useTimer && flushTimer != nil {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case <-flushTimer.C:
				flushTimer = nil
				if len(batch) == 0 {
					continue
				}
				toFlush := batch
				batch = nil
				if err := doFlush(toFlush); err != nil {
					c.logger.Error(err, "Failed to write batch on timer", "table", c.config.Table)
					return err
				}
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					return nil
				}
				batch = append(batch, msg)
				if len(batch) >= maxBatchSize {
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						c.logger.Error(err, "Failed to write batch", logkeys.MessageID, types.MessageID(msg), "table", c.config.Table)
						return err
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					return nil
				}
				batch = append(batch, msg)
				if len(batch) >= maxBatchSize {
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						c.logger.Error(err, "Failed to write batch", logkeys.MessageID, types.MessageID(msg), "table", c.config.Table)
						return err
					}
				}
			}
		}
	}
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
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, ": eof")
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
	return nil
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
	case *array.Binary:
		return a.Value(i)
	case *array.LargeString:
		return a.Value(i)
	default:
		return arr.ValueStr(i)
	}
}

// messagesToArrowTable builds an Arrow table with one "data" column (string) from messages.
func messagesToArrowTable(msgs []*types.Message) (arrow.Table, error) {
	mem := memory.DefaultAllocator
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
