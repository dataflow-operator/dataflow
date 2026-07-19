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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	timezoneFormatRFC3339     = "RFC3339"
	timezoneFormatRFC3339Nano = "RFC3339Nano"
	timezoneFormatUnixMilli   = "UnixMilli"
	epochMillisThreshold      = 1e12
)

var timezoneLayoutsWithZone = []string{
	time.RFC3339Nano,
	time.RFC3339,
}

var timezoneLayoutsWithoutZone = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// TimezoneTransformer converts temporal fields to a target timezone.
type TimezoneTransformer struct {
	config *v1.TimezoneTransformation
}

// NewTimezoneTransformer creates a new timezone transformer.
func NewTimezoneTransformer(config *v1.TimezoneTransformation) *TimezoneTransformer {
	return &TimezoneTransformer{config: config}
}

// Transform converts listed temporal fields to the configured timezone/format.
// Missing fields and JSON null are skipped. Unparseable values return a transform error.
// Non-JSON payloads are passed through unchanged. Metadata is preserved.
func (t *TimezoneTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !json.Valid(message.Data) {
		return []*types.Message{message}, nil
	}

	targetLoc, err := v1.LoadTimezoneLocation(t.config.Timezone)
	if err != nil {
		return nil, fmt.Errorf("timezone: %w", err)
	}

	sourceName := strings.TrimSpace(t.config.SourceTimezone)
	if sourceName == "" {
		sourceName = "UTC"
	}
	sourceLoc, err := v1.LoadTimezoneLocation(sourceName)
	if err != nil {
		return nil, fmt.Errorf("sourceTimezone: %w", err)
	}

	format := strings.TrimSpace(t.config.Format)
	if format == "" {
		format = timezoneFormatRFC3339Nano
	}

	jsonStr := string(message.Data)
	for _, field := range t.config.Fields {
		normalized := normalizeFieldPath(strings.TrimSpace(field))
		if normalized == "" {
			continue
		}

		result := gjson.Get(jsonStr, normalized)
		if !result.Exists() || result.Type == gjson.Null {
			continue
		}

		parsed, err := parseTemporalValue(result, sourceLoc)
		if err != nil {
			return nil, fmt.Errorf("timezone %q: %w", field, err)
		}

		converted := parsed.In(targetLoc)
		var out interface{}
		switch format {
		case timezoneFormatUnixMilli:
			out = converted.UnixMilli()
		case timezoneFormatRFC3339:
			out = converted.Format(time.RFC3339)
		case timezoneFormatRFC3339Nano:
			out = converted.Format(time.RFC3339Nano)
		default:
			return nil, fmt.Errorf("timezone unsupported format %q", format)
		}

		jsonStr, err = sjson.Set(jsonStr, normalized, out)
		if err != nil {
			return nil, fmt.Errorf("timezone set %q: %w", field, err)
		}
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}

func parseTemporalValue(result gjson.Result, sourceLoc *time.Location) (time.Time, error) {
	switch result.Type {
	case gjson.Number:
		return parseEpochNumber(result.Float())
	case gjson.String:
		s := strings.TrimSpace(result.String())
		if s == "" {
			return time.Time{}, fmt.Errorf("empty string is not a valid timestamp")
		}
		if t, err := parseTemporalString(s, sourceLoc); err == nil {
			return t, nil
		}
		if n, err := strconv.ParseFloat(s, 64); err == nil {
			return parseEpochNumber(n)
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", result.String())
	default:
		return time.Time{}, fmt.Errorf("cannot parse %s as timestamp", result.Type.String())
	}
}

func parseTemporalString(s string, sourceLoc *time.Location) (time.Time, error) {
	for _, layout := range timezoneLayoutsWithZone {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	for _, layout := range timezoneLayoutsWithoutZone {
		if t, err := time.ParseInLocation(layout, s, sourceLoc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format")
}

func parseEpochNumber(n float64) (time.Time, error) {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return time.Time{}, fmt.Errorf("invalid epoch number %v", n)
	}
	if math.Abs(n) >= epochMillisThreshold {
		return time.UnixMilli(int64(n)), nil
	}
	sec, frac := math.Modf(n)
	nsec := int64(math.Round(frac * 1e9))
	return time.Unix(int64(sec), nsec), nil
}
