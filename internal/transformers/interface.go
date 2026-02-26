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
	"encoding/json"

	"github.com/dataflow-operator/dataflow/internal/types"
)

// Transformer defines the interface for message transformations
type Transformer interface {
	// Transform transforms a message and returns one or more messages
	Transform(ctx context.Context, message *types.Message) ([]*types.Message, error)
}

// isValidJSON checks if data is valid JSON
func isValidJSON(data []byte) bool {
	var js interface{}
	return json.Unmarshal(data, &js) == nil
}
