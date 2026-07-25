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

package transformers

import (
	"context"

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// whenTransformer skips the inner transform (passthrough) when the when condition is false.
// Unlike filter, a false when does not drop the message — analogous to a Kafka Connect SMT predicate.
type whenTransformer struct {
	when  string
	inner Transformer
}

// Transform applies the inner transformer only when the when condition matches.
// An empty when always applies the inner transformer.
func (w *whenTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if w.when != "" && !evaluateCondition(message.Data, message.Metadata, w.when) {
		return []*types.Message{message}, nil
	}
	return w.inner.Transform(ctx, message)
}

// SetLogger forwards the logger to the inner transformer when it supports SetLogger.
func (w *whenTransformer) SetLogger(logger logr.Logger) {
	if lc, ok := w.inner.(interface{ SetLogger(logr.Logger) }); ok {
		lc.SetLogger(logger)
	}
}
