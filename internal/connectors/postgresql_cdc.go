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
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQLCDCSourceConnector implements SourceConnector via PostgreSQL logical replication (pgoutput).
type PostgreSQLCDCSourceConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config            *v1.PostgreSQLCDCSourceSpec
	sqlConn           *pgx.Conn
	replConn          *pgconn.PgConn
	cp                postgresCDCCheckpointHolder
	channelBufferSize int
	sourceType        string

	relationCache *postgresCDCRelationCache
	typeMap       *pgtype.Map
	columnFilter  *postgresCDCColumnFilter

	systemIdent *pglogrepl.IdentifySystemResult

	replMu sync.Mutex

	// readErrCh receives fatal errors from the read goroutine after Read returns; set during Read.
	readErrCh chan error
	// testReadLoop, if set, replaces readLoop in Read (unit tests only).
	testReadLoop func(ctx context.Context, msgChan chan *types.Message) error
	// testConnectRepl, if set, replaces pgconn.Connect during Connect (unit tests only).
	testConnectRepl func(ctx context.Context, connString string) (*pgconn.PgConn, error)
}

// NewPostgreSQLCDCSourceConnector creates a PostgreSQL CDC source connector.
func NewPostgreSQLCDCSourceConnector(config *v1.PostgreSQLCDCSourceSpec) *PostgreSQLCDCSourceConnector {
	return NewPostgreSQLCDCSourceConnectorWithOptions(config, nil)
}

// NewPostgreSQLCDCSourceConnectorWithOptions creates a PostgreSQL CDC source with checkpoint options.
func NewPostgreSQLCDCSourceConnectorWithOptions(config *v1.PostgreSQLCDCSourceSpec, opts *SourceConnectorOptions) *PostgreSQLCDCSourceConnector {
	c := &PostgreSQLCDCSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "postgresql-cdc", connectorRole: "source"},
		relationCache:     newPostgresCDCRelationCache(),
		typeMap:           pgtype.NewMap(),
		columnFilter:      newPostgresCDCColumnFilter(config),
	}
	if opts != nil {
		c.sourceType = opts.SourceType
		if c.sourceType == "" {
			c.sourceType = "postgresql-cdc"
		}
		var initial []byte
		if opts.InitialCheckpoint != nil {
			initial = opts.InitialCheckpoint
		}
		c.cp.init(opts.CheckpointStore, c.sourceType, config.SlotName, config.PublicationName, initial)
		if opts.ChannelBufferSize > 0 {
			c.channelBufferSize = opts.ChannelBufferSize
		} else {
			c.channelBufferSize = constants.DefaultChannelBufferSize
		}
	} else {
		c.channelBufferSize = constants.DefaultChannelBufferSize
		c.cp.init(nil, "postgresql-cdc", config.SlotName, config.PublicationName, nil)
	}
	return c
}

func (c *PostgreSQLCDCSourceConnector) heartbeatInterval() time.Duration {
	if c.config.HeartbeatIntervalSeconds != nil {
		sec := int(*c.config.HeartbeatIntervalSeconds)
		if sec <= 0 {
			return 0
		}
		return time.Duration(sec) * time.Second
	}
	return 10 * time.Second
}

// connectReplication opens a replication connection; testConnectRepl overrides pgconn.Connect in tests.
func (c *PostgreSQLCDCSourceConnector) connectReplication(ctx context.Context, connString string) (*pgconn.PgConn, error) {
	if c.testConnectRepl != nil {
		return c.testConnectRepl(ctx, connString)
	}
	return pgconn.Connect(ctx, connString)
}

// Connect validates PostgreSQL, bootstraps publication, and prepares replication.
func (c *PostgreSQLCDCSourceConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	c.logger.Info("Connecting to PostgreSQL CDC",
		"slot", c.config.SlotName,
		"publication", c.config.PublicationName,
		"tables", c.config.Tables,
	)

	conn, err := pgx.Connect(ctx, normalizePostgreSQLConnectionString(c.config.ConnectionString))
	if err != nil {
		c.RecordError("connect", "connection_error")
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	connectOK := false
	defer func() {
		if !connectOK {
			_ = conn.Close(ctx)
			c.sqlConn = nil
		}
	}()

	if err := checkPostgreSQLWalLevel(ctx, conn); err != nil {
		c.RecordError("connect", "wal_level_error")
		return err
	}

	tables := normalizePostgreSQLTableRefs(c.config.Tables)
	if postgresCDCCreatePublication(c.config) {
		if err := ensurePostgreSQLPublication(ctx, conn, c.config.PublicationName, tables); err != nil {
			c.RecordError("connect", "publication_error")
			return err
		}
	}
	if err := checkPostgreSQLReplicaIdentity(ctx, conn, tables); err != nil {
		c.RecordError("connect", "replica_identity_error")
		return err
	}

	replConn, err := c.connectReplication(ctx, replicationConnectionString(c.config.ConnectionString))
	if err != nil {
		c.RecordError("connect", "replication_connection_error")
		return fmt.Errorf("failed to connect for replication: %w", err)
	}
	defer func() { _ = replConn.Close(ctx) }()

	sysident, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		c.RecordError("connect", "identify_system_error")
		return fmt.Errorf("identify system: %w", err)
	}
	c.systemIdent = &sysident

	exists, err := slotExists(ctx, conn, c.config.SlotName)
	if err != nil {
		return fmt.Errorf("check replication slot: %w", err)
	}
	if !exists {
		if !postgresCDCCreateSlot(c.config) {
			return fmt.Errorf("replication slot %q does not exist", c.config.SlotName)
		}
		_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, c.config.SlotName, postgresCDCPlugin(c.config), pglogrepl.CreateReplicationSlotOptions{})
		if err != nil {
			c.RecordError("connect", "create_slot_error")
			return fmt.Errorf("create replication slot: %w", err)
		}
		c.logger.Info("Created replication slot", "slot", c.config.SlotName)
	}

	c.sqlConn = conn
	connectOK = true

	c.cp.onAdvance = func(lsn pglogrepl.LSN) {
		c.sendStandbyStatusUpdate(lsn)
	}

	c.SetConnectionStatus(true)
	c.logger.Info("Successfully connected to PostgreSQL CDC", "slot", c.config.SlotName)
	return nil
}

// Read streams logical replication changes.
func (c *PostgreSQLCDCSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if c.sqlConn == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *types.Message, c.channelBufferSize)
	errCh := make(chan error, constants.DefaultSingleValueChannelBufferSize)
	c.readErrCh = errCh

	readLoopFn := c.readLoop
	if c.testReadLoop != nil {
		readLoopFn = c.testReadLoop
	}

	go func() {
		defer close(msgChan)
		if err := readLoopFn(ctx, msgChan); err != nil && ctx.Err() == nil {
			errWrap := fmt.Errorf("postgres CDC read error: %w", err)
			c.logger.Error(errWrap, "PostgreSQL CDC read loop failed")
			c.RecordError("read", "stream_error")
			select {
			case errCh <- errWrap:
			default:
				c.logger.Error(errWrap, "PostgreSQL CDC read error dropped (error channel full)")
			}
		}
	}()
	return msgChan, nil
}

// ReadErrors implements SourceReadErrors. Returns the error channel created by the last Read call.
func (c *PostgreSQLCDCSourceConnector) ReadErrors() <-chan error {
	return c.readErrCh
}

func (c *PostgreSQLCDCSourceConnector) readLoop(ctx context.Context, msgChan chan *types.Message) error {
	if c.shouldRunSnapshot() {
		c.logger.Info("Running initial CDC snapshot", "tables", c.config.Tables)
		if err := c.runInitialSnapshot(ctx, msgChan); err != nil {
			return err
		}
	}

	replConn, err := pgconn.Connect(ctx, replicationConnectionString(c.config.ConnectionString))
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	c.replMu.Lock()
	c.replConn = replConn
	c.replMu.Unlock()
	defer func() {
		c.replMu.Lock()
		if c.replConn != nil {
			_ = c.replConn.Close(ctx)
			c.replConn = nil
		}
		c.replMu.Unlock()
	}()

	startLSN := c.cp.startLSN()
	if startLSN == 0 && c.systemIdent != nil {
		startLSN = c.systemIdent.XLogPos
	}

	if err := pglogrepl.StartReplication(ctx, replConn, c.config.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pgoutputPluginArgs(c.config.PublicationName),
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	c.logger.Info("Logical replication started", "slot", c.config.SlotName, "startLSN", startLSN.String())

	clientLSN := startLSN
	if c.systemIdent != nil && c.systemIdent.XLogPos > clientLSN {
		clientLSN = c.systemIdent.XLogPos
	}

	heartbeat := c.heartbeatInterval()
	var nextStandbyDeadline time.Time
	if heartbeat > 0 {
		nextStandbyDeadline = time.Now().Add(heartbeat)
	}

	tables := normalizePostgreSQLTableRefs(c.config.Tables)
	pkCol := postgresCDCPrimaryKeyColumn(c.config)
	inStream := false
	var txnMessages []*types.Message
	var txnCommitLSN pglogrepl.LSN

	for {
		if heartbeat > 0 && !nextStandbyDeadline.IsZero() && time.Now().After(nextStandbyDeadline) {
			c.sendStandbyStatusUpdate(clientLSN)
			nextStandbyDeadline = time.Now().Add(heartbeat)
		}

		receiveCtx := ctx
		var cancel context.CancelFunc
		if heartbeat > 0 && !nextStandbyDeadline.IsZero() {
			receiveCtx, cancel = context.WithDeadline(ctx, nextStandbyDeadline)
		}
		rawMsg, err := replConn.ReceiveMessage(receiveCtx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive replication message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ServerWALEnd > clientLSN {
				clientLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				c.sendStandbyStatusUpdate(clientLSN)
				if heartbeat > 0 {
					nextStandbyDeadline = time.Now().Add(heartbeat)
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			if xld.ServerWALEnd > clientLSN {
				clientLSN = xld.ServerWALEnd
			}

			if err := c.processWALData(ctx, msgChan, xld.WALData, &inStream, tables, pkCol, &txnMessages, &txnCommitLSN); err != nil {
				return err
			}
		}
	}
}

func (c *PostgreSQLCDCSourceConnector) processWALData(
	ctx context.Context,
	msgChan chan *types.Message,
	walData []byte,
	inStream *bool,
	tables []string,
	pkCol string,
	txnMessages *[]*types.Message,
	txnCommitLSN *pglogrepl.LSN,
) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return fmt.Errorf("parse logical message: %w", err)
	}

	flushTxn := func() error {
		if len(*txnMessages) == 0 {
			*txnCommitLSN = 0
			return nil
		}
		lsn := *txnCommitLSN
		for _, msg := range *txnMessages {
			msg.Ack = c.cp.makeAck(lsn)
			select {
			case msgChan <- msg:
				c.RecordMessageRead()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		*txnMessages = nil
		*txnCommitLSN = 0
		return nil
	}

	switch m := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		if c.relationCache.put(m) {
			c.logger.Info("PostgreSQL CDC relation schema refreshed",
				"table", relationTableRef(m),
				"relationID", m.RelationID,
				"columns", len(m.Columns),
			)
		}

	case *pglogrepl.BeginMessage:
		if err := flushTxn(); err != nil {
			return err
		}

	case *pglogrepl.CommitMessage:
		if m.CommitLSN != 0 {
			*txnCommitLSN = m.CommitLSN
		} else if m.TransactionEndLSN != 0 {
			*txnCommitLSN = m.TransactionEndLSN
		}
		return flushTxn()

	case *pglogrepl.StreamCommitMessageV2:
		if m.CommitLSN != 0 {
			*txnCommitLSN = m.CommitLSN
		}
		*inStream = false
		return flushTxn()

	case *pglogrepl.StreamAbortMessageV2:
		*txnMessages = nil
		*txnCommitLSN = 0
		*inStream = false

	case *pglogrepl.InsertMessageV2:
		msg, err := c.buildChangeMessage(m.RelationID, m.Tuple, nil, "insert", tables, pkCol)
		if err != nil {
			return err
		}
		if msg != nil {
			*txnMessages = append(*txnMessages, msg)
		}

	case *pglogrepl.UpdateMessageV2:
		msg, err := c.buildChangeMessage(m.RelationID, m.NewTuple, m.OldTuple, "update", tables, pkCol)
		if err != nil {
			return err
		}
		if msg != nil {
			*txnMessages = append(*txnMessages, msg)
		}

	case *pglogrepl.DeleteMessageV2:
		msg, err := c.buildChangeMessage(m.RelationID, m.OldTuple, nil, "delete", tables, pkCol)
		if err != nil {
			return err
		}
		if msg != nil {
			*txnMessages = append(*txnMessages, msg)
		}

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false

	case *pglogrepl.TruncateMessageV2:
		c.logger.V(1).Info("Truncate received", "relationIDs", m.RelationIDs)

	default:
		c.logger.V(1).Info("Ignoring replication message", "type", logicalMsg.Type())
	}
	return nil
}

func (c *PostgreSQLCDCSourceConnector) buildChangeMessage(
	relationID uint32,
	tuple *pglogrepl.TupleData,
	oldTuple *pglogrepl.TupleData,
	operation string,
	tables []string,
	pkCol string,
) (*types.Message, error) {
	rel, ok := c.relationCache.get(relationID)
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", relationID)
	}
	tableRef := relationTableRef(rel)
	if !tableInConfig(tableRef, tables) {
		return nil, nil
	}

	var after, before map[string]interface{}
	var err error

	switch operation {
	case "insert":
		after, err = tupleToRow(rel, tuple, c.typeMap, c.columnFilter)
	case "update":
		after, err = tupleToRow(rel, tuple, c.typeMap, c.columnFilter)
		if err == nil && oldTuple != nil {
			before, err = tupleToRow(rel, oldTuple, c.typeMap, c.columnFilter)
		}
	case "delete":
		tupleData := tuple
		if tupleData == nil {
			tupleData = oldTuple
		}
		before, err = tupleToRow(rel, tupleData, c.typeMap, c.columnFilter)
	default:
		return nil, fmt.Errorf("unsupported CDC operation %q", operation)
	}
	if err != nil {
		return nil, err
	}

	return c.buildCDCMessage(after, before, tableRef, operation, 0, pkCol, false)
}

func (c *PostgreSQLCDCSourceConnector) sendStandbyStatusUpdate(lsn pglogrepl.LSN) {
	c.replMu.Lock()
	conn := c.replConn
	c.replMu.Unlock()
	if conn == nil || lsn == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
	}); err != nil {
		c.logger.V(1).Info("Failed to send standby status update", "error", err, "lsn", lsn.String())
	}
}

// Close closes SQL and replication connections.
func (c *PostgreSQLCDCSourceConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()

	c.SetConnectionStatus(false)
	c.logger.Info("Closing PostgreSQL CDC source", "slot", c.config.SlotName)

	c.replMu.Lock()
	if c.replConn != nil {
		_ = c.replConn.Close(context.Background())
		c.replConn = nil
	}
	c.replMu.Unlock()

	if c.sqlConn != nil {
		err := c.sqlConn.Close(context.Background())
		c.sqlConn = nil
		return err
	}
	return nil
}
