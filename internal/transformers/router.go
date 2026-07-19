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

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// RouterTransformer routes messages to different sinks based on conditions
type RouterTransformer struct {
	config *v1.RouterTransformation
	logger logr.Logger
}

// NewRouterTransformer creates a new router transformer
func NewRouterTransformer(config *v1.RouterTransformation) *RouterTransformer {
	return &RouterTransformer{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the transformer (used by processor to inject logr)
func (r *RouterTransformer) SetLogger(logger logr.Logger) {
	r.logger = logger
}

// Transform routes messages based on conditions.
// Returns messages with routing metadata. Condition syntax matches filter:
// truthiness, ==, and !=.
func (r *RouterTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	r.logger.V(1).Info("Router processing message",
		"routesCount", len(r.config.Routes),
		"dataSize", len(message.Data))

	for i, route := range r.config.Routes {
		condition := route.Condition
		r.logger.V(1).Info("Router checking route",
			"routeIndex", i,
			"condition", condition)

		if !evaluateCondition(message.Data, condition) {
			r.logger.V(1).Info("Router condition did not match",
				"condition", condition)
			continue
		}

		newMsg := types.NewMessage(message.Data)
		newMsg.Metadata = make(map[string]interface{})
		for k, v := range message.Metadata {
			newMsg.Metadata[k] = v
		}
		newMsg.Metadata["routed_condition"] = route.Condition
		newMsg.Timestamp = message.Timestamp
		r.logger.V(1).Info("Router message routed",
			"condition", route.Condition)
		return []*types.Message{newMsg}, nil
	}

	// No route matched, return original message
	return []*types.Message{message}, nil
}
