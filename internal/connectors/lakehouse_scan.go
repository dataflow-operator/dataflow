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

	"github.com/dataflow-operator/dataflow/internal/types"
)

// lakehousePollLimits caps work per poll cycle for Iceberg/Nessie sources.
type lakehousePollLimits struct {
	maxRows  int64
	maxBytes int64
}

func lakehousePollLimitsFrom(maxRows, maxBytes *int32) lakehousePollLimits {
	var l lakehousePollLimits
	if maxRows != nil && *maxRows > 0 {
		l.maxRows = int64(*maxRows)
	}
	if maxBytes != nil && *maxBytes > 0 {
		l.maxBytes = int64(*maxBytes)
	}
	return l
}

func (l lakehousePollLimits) active() bool {
	return l.maxRows > 0 || l.maxBytes > 0
}

type lakehouseEmitStats struct {
	Emitted    int
	Bytes      int64
	HitLimit   bool
	NextOffset int64 // skipRows + Emitted when HitLimit; 0 when complete
}

func sendLakehouseMessages(
	ctx context.Context,
	msgChan chan *types.Message,
	msgs []*types.Message,
	reportFill func(*types.Message),
) error {
	for _, msg := range msgs {
		if reportFill != nil {
			reportFill(msg)
		}
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
