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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type hiveMetastore interface {
	GetTable(ctx context.Context, db, table string) (hiveTableMeta, error)
	GetPartitions(ctx context.Context, db, table string) ([]hivePartitionMeta, error)
	AddPartition(ctx context.Context, part hivePartitionMeta) error
	CreateTable(ctx context.Context, table hiveTableMeta) error
	Close() error
}

type hiveMetastoreError struct {
	kind string
	msg  string
}

func (e *hiveMetastoreError) Error() string { return e.msg }

func (e *hiveMetastoreError) Is(target error) bool {
	switch target {
	case errHiveNotFound:
		return e.kind == "not_found"
	case errHiveAlreadyExists:
		return e.kind == "already_exists"
	default:
		return false
	}
}

type thriftHiveClient struct {
	mu    sync.Mutex
	seq   int32
	sock  *thrift.TSocket
	trans *thrift.TBufferedTransport
	proto *thrift.TBinaryProtocol
}

func metastoreAddr(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("metastore URI is empty")
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", fmt.Errorf("invalid metastore URI: %w", err)
		}
		switch strings.ToLower(u.Scheme) {
		case "thrift", "tcp":
			host := u.Host
			if host == "" {
				return "", fmt.Errorf("metastore URI %q has no host", raw)
			}
			if u.Port() == "" {
				host += ":9083"
			}
			return host, nil
		default:
			return "", fmt.Errorf("unsupported metastore URI scheme %q", u.Scheme)
		}
	}
	if !strings.Contains(raw, ":") {
		raw += ":9083"
	}
	return raw, nil
}

func dialThriftHiveMetastore(ctx context.Context, uri string) (hiveMetastore, error) {
	addr, err := metastoreAddr(uri)
	if err != nil {
		return nil, err
	}
	conf := &thrift.TConfiguration{
		ConnectTimeout:     15 * time.Second,
		SocketTimeout:      60 * time.Second,
		TBinaryStrictRead:  thrift.BoolPtr(true),
		TBinaryStrictWrite: thrift.BoolPtr(true),
	}
	sock := thrift.NewTSocketConf(addr, conf)
	if err := sock.Open(); err != nil {
		return nil, fmt.Errorf("connect Hive Metastore %s: %w", addr, err)
	}
	trans := thrift.NewTBufferedTransport(sock, 8192)
	return &thriftHiveClient{
		sock:  sock,
		trans: trans,
		proto: thrift.NewTBinaryProtocolConf(trans, conf),
	}, nil
}

func (c *thriftHiveClient) Close() error {
	if c == nil || c.sock == nil {
		return nil
	}
	return c.sock.Close()
}

func (c *thriftHiveClient) GetTable(ctx context.Context, db, table string) (hiveTableMeta, error) {
	var out hiveTableMeta
	err := c.call(ctx, "get_table", func(p *thrift.TBinaryProtocol) error {
		if err := writeStringField(ctx, p, "dbname", 1, db); err != nil {
			return err
		}
		return writeStringField(ctx, p, "tbl_name", 2, table)
	}, func(typ thrift.TType) error {
		if typ != thrift.STRUCT {
			return skipThrift(ctx, c.proto, typ)
		}
		meta, err := readHiveTable(ctx, c.proto)
		if err != nil {
			return err
		}
		out = meta
		return nil
	}, map[int16]string{2: "not_found"})
	return out, err
}

func (c *thriftHiveClient) GetPartitions(ctx context.Context, db, table string) ([]hivePartitionMeta, error) {
	var out []hivePartitionMeta
	err := c.call(ctx, "get_partitions", func(p *thrift.TBinaryProtocol) error {
		if err := writeStringField(ctx, p, "db_name", 1, db); err != nil {
			return err
		}
		if err := writeStringField(ctx, p, "tbl_name", 2, table); err != nil {
			return err
		}
		if err := p.WriteFieldBegin(ctx, "max_parts", thrift.I16, 3); err != nil {
			return err
		}
		if err := p.WriteI16(ctx, -1); err != nil {
			return err
		}
		return p.WriteFieldEnd(ctx)
	}, func(typ thrift.TType) error {
		if typ != thrift.LIST {
			return skipThrift(ctx, c.proto, typ)
		}
		parts, err := readHivePartitionList(ctx, c.proto)
		if err != nil {
			return err
		}
		out = parts
		return nil
	}, map[int16]string{1: "not_found"})
	return out, err
}

func (c *thriftHiveClient) AddPartition(ctx context.Context, part hivePartitionMeta) error {
	return c.call(ctx, "add_partition", func(p *thrift.TBinaryProtocol) error {
		return writePartitionField(ctx, p, "new_part", 1, part)
	}, nil, map[int16]string{2: "already_exists"})
}

func (c *thriftHiveClient) CreateTable(ctx context.Context, table hiveTableMeta) error {
	return c.call(ctx, "create_table", func(p *thrift.TBinaryProtocol) error {
		return writeTableField(ctx, p, "tbl", 1, table)
	}, nil, map[int16]string{1: "already_exists"})
}

func (c *thriftHiveClient) call(ctx context.Context, method string, writeArgs func(*thrift.TBinaryProtocol) error, readSuccess func(thrift.TType) error, fieldKind map[int16]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	p := c.proto
	if err := p.WriteMessageBegin(ctx, method, thrift.CALL, c.seq); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := p.WriteStructBegin(ctx, method+"_args"); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := writeArgs(p); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := p.WriteMessageEnd(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := c.trans.Flush(ctx); err != nil {
		return wrapMetastoreIO(err)
	}

	_, typ, _, err := p.ReadMessageBegin(ctx)
	if err != nil {
		return wrapMetastoreIO(err)
	}
	if typ == thrift.EXCEPTION {
		exc := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "")
		if rerr := exc.Read(ctx, p); rerr != nil {
			return wrapMetastoreIO(rerr)
		}
		_ = p.ReadMessageEnd(ctx)
		return fmt.Errorf("Hive Metastore SASL/Kerberos is not supported (binary protocol, no SASL): %s", exc.Error())
	}
	if typ != thrift.REPLY {
		return fmt.Errorf("Hive Metastore returned message type %d for %s", typ, method)
	}
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	var callErr error
	for {
		_, ftyp, fid, ferr := p.ReadFieldBegin(ctx)
		if ferr != nil {
			return wrapMetastoreIO(ferr)
		}
		if ftyp == thrift.STOP {
			break
		}
		if fid == 0 && readSuccess != nil {
			if err := readSuccess(ftyp); err != nil {
				return err
			}
		} else if fid != 0 {
			msg, rerr := readExceptionMessage(ctx, p, ftyp)
			if rerr != nil {
				return rerr
			}
			kind := "other"
			if fieldKind != nil {
				if k, ok := fieldKind[fid]; ok {
					kind = k
				}
			}
			if msg == "" {
				msg = fmt.Sprintf("Hive Metastore %s failed (field %d)", method, fid)
			}
			callErr = &hiveMetastoreError{kind: kind, msg: msg}
		} else if err := skipThrift(ctx, p, ftyp); err != nil {
			return err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return wrapMetastoreIO(err)
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	if err := p.ReadMessageEnd(ctx); err != nil {
		return wrapMetastoreIO(err)
	}
	return callErr
}

func wrapMetastoreIO(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("Hive Metastore call failed (binary protocol without SASL or framed transport): %w", err)
}

func writeStringField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, value string) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRING, id); err != nil {
		return err
	}
	if err := p.WriteString(ctx, value); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeTableField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, table hiveTableMeta) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRUCT, id); err != nil {
		return err
	}
	if err := writeHiveTable(ctx, p, table); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writePartitionField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, part hivePartitionMeta) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRUCT, id); err != nil {
		return err
	}
	if err := writeHivePartition(ctx, p, part); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeHiveTable(ctx context.Context, p *thrift.TBinaryProtocol, table hiveTableMeta) error {
	if err := p.WriteStructBegin(ctx, "Table"); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "tableName", 1, table.Name); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "dbName", 2, table.DB); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "owner", 3, "dataflow"); err != nil {
		return err
	}
	now := int32(time.Now().Unix())
	if err := writeI32Field(ctx, p, "createTime", 4, now); err != nil {
		return err
	}
	if err := writeI32Field(ctx, p, "lastAccessTime", 5, now); err != nil {
		return err
	}
	if err := writeI32Field(ctx, p, "retention", 6, 0); err != nil {
		return err
	}
	sd := storageFromTable(table)
	if err := writeStorageField(ctx, p, 7, sd); err != nil {
		return err
	}
	if err := writeFieldSchemaListField(ctx, p, "partitionKeys", 8, table.PartitionKeys); err != nil {
		return err
	}
	params := table.Parameters
	if params == nil {
		params = map[string]string{}
	}
	if err := writeStringMapField(ctx, p, "parameters", 9, params); err != nil {
		return err
	}
	tableType := table.TableType
	if tableType == "" {
		tableType = "EXTERNAL_TABLE"
	}
	if err := writeStringField(ctx, p, "tableType", 12, tableType); err != nil {
		return err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	return p.WriteStructEnd(ctx)
}

func writeHivePartition(ctx context.Context, p *thrift.TBinaryProtocol, part hivePartitionMeta) error {
	if err := p.WriteStructBegin(ctx, "Partition"); err != nil {
		return err
	}
	if err := writeStringListField(ctx, p, "values", 1, part.Values); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "dbName", 2, part.DB); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "tableName", 3, part.Table); err != nil {
		return err
	}
	now := int32(time.Now().Unix())
	if err := writeI32Field(ctx, p, "createTime", 4, now); err != nil {
		return err
	}
	sd := hiveTableMeta{Location: part.Location, InputFormat: hiveParquetInputFormat, OutputFormat: hiveParquetOutputFormat, SerDe: hiveParquetSerDe}
	if err := writeStorageField(ctx, p, 6, storageFromTable(sd)); err != nil {
		return err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	return p.WriteStructEnd(ctx)
}

type hiveStorageDesc struct {
	Columns      []hiveField
	Location     string
	InputFormat  string
	OutputFormat string
	SerDe        string
}

func storageFromTable(table hiveTableMeta) hiveStorageDesc {
	in := table.InputFormat
	if in == "" {
		in = hiveParquetInputFormat
	}
	out := table.OutputFormat
	if out == "" {
		out = hiveParquetOutputFormat
	}
	serde := table.SerDe
	if serde == "" {
		serde = hiveParquetSerDe
	}
	return hiveStorageDesc{
		Columns:      table.Columns,
		Location:     table.Location,
		InputFormat:  in,
		OutputFormat: out,
		SerDe:        serde,
	}
}

func writeStorageField(ctx context.Context, p *thrift.TBinaryProtocol, id int16, sd hiveStorageDesc) error {
	if err := p.WriteFieldBegin(ctx, "sd", thrift.STRUCT, id); err != nil {
		return err
	}
	if err := p.WriteStructBegin(ctx, "StorageDescriptor"); err != nil {
		return err
	}
	if err := writeFieldSchemaListField(ctx, p, "cols", 1, sd.Columns); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "location", 2, sd.Location); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "inputFormat", 3, sd.InputFormat); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "outputFormat", 4, sd.OutputFormat); err != nil {
		return err
	}
	if err := p.WriteFieldBegin(ctx, "compressed", thrift.BOOL, 5); err != nil {
		return err
	}
	if err := p.WriteBool(ctx, false); err != nil {
		return err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return err
	}
	if err := writeI32Field(ctx, p, "numBuckets", 6, 0); err != nil {
		return err
	}
	if err := p.WriteFieldBegin(ctx, "serdeInfo", thrift.STRUCT, 7); err != nil {
		return err
	}
	if err := p.WriteStructBegin(ctx, "SerDeInfo"); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "name", 1, "dataflow"); err != nil {
		return err
	}
	if err := writeStringField(ctx, p, "serializationLib", 2, sd.SerDe); err != nil {
		return err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeI32Field(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, value int32) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.I32, id); err != nil {
		return err
	}
	if err := p.WriteI32(ctx, value); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeStringListField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, values []string) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.LIST, id); err != nil {
		return err
	}
	if err := p.WriteListBegin(ctx, thrift.STRING, len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if err := p.WriteString(ctx, v); err != nil {
			return err
		}
	}
	if err := p.WriteListEnd(ctx); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeFieldSchemaListField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, cols []hiveField) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.LIST, id); err != nil {
		return err
	}
	if err := p.WriteListBegin(ctx, thrift.STRUCT, len(cols)); err != nil {
		return err
	}
	for _, col := range cols {
		if err := p.WriteStructBegin(ctx, "FieldSchema"); err != nil {
			return err
		}
		if err := writeStringField(ctx, p, "name", 1, col.Name); err != nil {
			return err
		}
		if err := writeStringField(ctx, p, "type", 2, col.Type); err != nil {
			return err
		}
		if err := p.WriteFieldStop(ctx); err != nil {
			return err
		}
		if err := p.WriteStructEnd(ctx); err != nil {
			return err
		}
	}
	if err := p.WriteListEnd(ctx); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func writeStringMapField(ctx context.Context, p *thrift.TBinaryProtocol, name string, id int16, values map[string]string) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.MAP, id); err != nil {
		return err
	}
	if err := p.WriteMapBegin(ctx, thrift.STRING, thrift.STRING, len(values)); err != nil {
		return err
	}
	for k, v := range values {
		if err := p.WriteString(ctx, k); err != nil {
			return err
		}
		if err := p.WriteString(ctx, v); err != nil {
			return err
		}
	}
	if err := p.WriteMapEnd(ctx); err != nil {
		return err
	}
	return p.WriteFieldEnd(ctx)
}

func readHiveTable(ctx context.Context, p *thrift.TBinaryProtocol) (hiveTableMeta, error) {
	var table hiveTableMeta
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return table, err
	}
	for {
		_, typ, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return table, err
		}
		if typ == thrift.STOP {
			break
		}
		switch id {
		case 1:
			table.Name, err = readStringValue(ctx, p, typ)
		case 2:
			table.DB, err = readStringValue(ctx, p, typ)
		case 7:
			var sd hiveStorageDesc
			sd, err = readStorageValue(ctx, p, typ)
			table.Columns = sd.Columns
			table.Location = sd.Location
			table.InputFormat = sd.InputFormat
			table.OutputFormat = sd.OutputFormat
			table.SerDe = sd.SerDe
		case 8:
			table.PartitionKeys, err = readFieldSchemaListValue(ctx, p, typ)
		case 9:
			table.Parameters, err = readStringMapValue(ctx, p, typ)
		case 12:
			table.TableType, err = readStringValue(ctx, p, typ)
		default:
			err = skipThrift(ctx, p, typ)
		}
		if err != nil {
			return table, err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return table, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return table, err
	}
	return table, nil
}

func readHivePartitionList(ctx context.Context, p *thrift.TBinaryProtocol) ([]hivePartitionMeta, error) {
	elem, n, err := p.ReadListBegin(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]hivePartitionMeta, 0, n)
	for i := 0; i < n; i++ {
		if elem != thrift.STRUCT {
			if err := skipThrift(ctx, p, elem); err != nil {
				return nil, err
			}
			continue
		}
		part, err := readHivePartition(ctx, p)
		if err != nil {
			return nil, err
		}
		out = append(out, part)
	}
	if err := p.ReadListEnd(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func readHivePartition(ctx context.Context, p *thrift.TBinaryProtocol) (hivePartitionMeta, error) {
	var part hivePartitionMeta
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return part, err
	}
	for {
		_, typ, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return part, err
		}
		if typ == thrift.STOP {
			break
		}
		switch id {
		case 1:
			part.Values, err = readStringListValue(ctx, p, typ)
		case 2:
			part.DB, err = readStringValue(ctx, p, typ)
		case 3:
			part.Table, err = readStringValue(ctx, p, typ)
		case 6:
			var sd hiveStorageDesc
			sd, err = readStorageValue(ctx, p, typ)
			part.Location = sd.Location
		default:
			err = skipThrift(ctx, p, typ)
		}
		if err != nil {
			return part, err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return part, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return part, err
	}
	return part, nil
}

func readStorageValue(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) (hiveStorageDesc, error) {
	var sd hiveStorageDesc
	if typ != thrift.STRUCT {
		return sd, skipThrift(ctx, p, typ)
	}
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return sd, err
	}
	for {
		_, ftyp, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return sd, err
		}
		if ftyp == thrift.STOP {
			break
		}
		switch id {
		case 1:
			sd.Columns, err = readFieldSchemaListValue(ctx, p, ftyp)
		case 2:
			sd.Location, err = readStringValue(ctx, p, ftyp)
		case 3:
			sd.InputFormat, err = readStringValue(ctx, p, ftyp)
		case 4:
			sd.OutputFormat, err = readStringValue(ctx, p, ftyp)
		case 7:
			sd.SerDe, err = readSerDeLib(ctx, p, ftyp)
		default:
			err = skipThrift(ctx, p, ftyp)
		}
		if err != nil {
			return sd, err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return sd, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return sd, err
	}
	return sd, nil
}

func readSerDeLib(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) (string, error) {
	if typ != thrift.STRUCT {
		return "", skipThrift(ctx, p, typ)
	}
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return "", err
	}
	var lib string
	for {
		_, ftyp, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return "", err
		}
		if ftyp == thrift.STOP {
			break
		}
		if id == 2 {
			lib, err = readStringValue(ctx, p, ftyp)
		} else {
			err = skipThrift(ctx, p, ftyp)
		}
		if err != nil {
			return "", err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return "", err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return "", err
	}
	return lib, nil
}

func readFieldSchemaListValue(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) ([]hiveField, error) {
	if typ != thrift.LIST {
		return nil, skipThrift(ctx, p, typ)
	}
	elem, n, err := p.ReadListBegin(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]hiveField, 0, n)
	for i := 0; i < n; i++ {
		if elem != thrift.STRUCT {
			if err := skipThrift(ctx, p, elem); err != nil {
				return nil, err
			}
			continue
		}
		col, err := readFieldSchema(ctx, p)
		if err != nil {
			return nil, err
		}
		out = append(out, col)
	}
	if err := p.ReadListEnd(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func readFieldSchema(ctx context.Context, p *thrift.TBinaryProtocol) (hiveField, error) {
	var col hiveField
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return col, err
	}
	for {
		_, typ, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return col, err
		}
		if typ == thrift.STOP {
			break
		}
		switch id {
		case 1:
			col.Name, err = readStringValue(ctx, p, typ)
		case 2:
			col.Type, err = readStringValue(ctx, p, typ)
		default:
			err = skipThrift(ctx, p, typ)
		}
		if err != nil {
			return col, err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return col, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return col, err
	}
	return col, nil
}

func readStringListValue(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) ([]string, error) {
	if typ != thrift.LIST {
		return nil, skipThrift(ctx, p, typ)
	}
	elem, n, err := p.ReadListBegin(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := readStringValue(ctx, p, elem)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := p.ReadListEnd(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func readStringMapValue(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) (map[string]string, error) {
	if typ != thrift.MAP {
		return nil, skipThrift(ctx, p, typ)
	}
	kt, vt, n, err := p.ReadMapBegin(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k, err := readStringValue(ctx, p, kt)
		if err != nil {
			return nil, err
		}
		v, err := readStringValue(ctx, p, vt)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	if err := p.ReadMapEnd(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func readStringValue(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) (string, error) {
	if typ != thrift.STRING {
		return "", skipThrift(ctx, p, typ)
	}
	return p.ReadString(ctx)
}

func readExceptionMessage(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) (string, error) {
	if typ != thrift.STRUCT {
		return "", skipThrift(ctx, p, typ)
	}
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return "", err
	}
	var msg string
	for {
		_, ftyp, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return "", err
		}
		if ftyp == thrift.STOP {
			break
		}
		if id == 1 {
			msg, err = readStringValue(ctx, p, ftyp)
		} else {
			err = skipThrift(ctx, p, ftyp)
		}
		if err != nil {
			return "", err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return "", err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return "", err
	}
	return msg, nil
}

func skipThrift(ctx context.Context, p *thrift.TBinaryProtocol, typ thrift.TType) error {
	switch typ {
	case thrift.BOOL:
		_, err := p.ReadBool(ctx)
		return err
	case thrift.BYTE:
		_, err := p.ReadByte(ctx)
		return err
	case thrift.DOUBLE:
		_, err := p.ReadDouble(ctx)
		return err
	case thrift.I16:
		_, err := p.ReadI16(ctx)
		return err
	case thrift.I32:
		_, err := p.ReadI32(ctx)
		return err
	case thrift.I64:
		_, err := p.ReadI64(ctx)
		return err
	case thrift.STRING:
		_, err := p.ReadString(ctx)
		return err
	case thrift.STRUCT:
		if _, err := p.ReadStructBegin(ctx); err != nil {
			return err
		}
		for {
			_, ftyp, _, err := p.ReadFieldBegin(ctx)
			if err != nil {
				return err
			}
			if ftyp == thrift.STOP {
				return p.ReadStructEnd(ctx)
			}
			if err := skipThrift(ctx, p, ftyp); err != nil {
				return err
			}
			if err := p.ReadFieldEnd(ctx); err != nil {
				return err
			}
		}
	case thrift.MAP:
		kt, vt, n, err := p.ReadMapBegin(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := skipThrift(ctx, p, kt); err != nil {
				return err
			}
			if err := skipThrift(ctx, p, vt); err != nil {
				return err
			}
		}
		return p.ReadMapEnd(ctx)
	case thrift.SET:
		elem, n, err := p.ReadSetBegin(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := skipThrift(ctx, p, elem); err != nil {
				return err
			}
		}
		return p.ReadSetEnd(ctx)
	case thrift.LIST:
		elem, n, err := p.ReadListBegin(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := skipThrift(ctx, p, elem); err != nil {
				return err
			}
		}
		return p.ReadListEnd(ctx)
	default:
		return fmt.Errorf("unsupported Thrift type %d", typ)
	}
}
