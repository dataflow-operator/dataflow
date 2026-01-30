# Transformations

DataFlow Operator supports various message transformations that are applied sequentially to each message in the order specified in the configuration. All transformations support JSONPath for working with nested data structures.

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/transformations.md).

## Transformation Overview

| Transformation | Description | Input | Output |
|----------------|------------|-------|--------|
| Timestamp | Adds timestamp | 1 message | 1 message |
| Flatten | Expands arrays | 1 message | N messages |
| Filter | Filters by condition | 1 message | 0 or 1 message |
| Mask | Masks data | 1 message | 1 message |
| Router | Routes to different sinks | 1 message | 0 or 1 message |
| Select | Selects fields | 1 message | 1 message |
| Remove | Removes fields | 1 message | 1 message |
| SnakeCase | Converts keys to snake_case | 1 message | 1 message |
| CamelCase | Converts keys to CamelCase | 1 message | 1 message |
| ReplaceField | Renames fields | 1 message | 1 message |
| HeaderFrom | Extracts data from Kafka headers | 1 message | 1 message |

## Timestamp

Adds a timestamp field to each message. Useful for tracking message processing time.

### Configuration

```yaml
transformations:
  - type: timestamp
    timestamp:
      # Field name for timestamp (optional, default: created_at)
      fieldName: created_at
      # Timestamp format (optional, default: RFC3339)
      format: RFC3339
```

### Supported Formats

- `RFC3339` - `2006-01-02T15:04:05Z07:00` (default)
- `RFC3339Nano` - `2006-01-02T15:04:05.999999999Z07:00`
- `Unix` - Unix timestamp in seconds
- `UnixMilli` - Unix timestamp in milliseconds
- Any custom Go time package format

## Flatten

Expands an array into separate messages, preserving all parent fields. Useful for processing nested data structures.

### Configuration

```yaml
transformations:
  - type: flatten
    flatten:
      # JSONPath to array to expand (required)
      field: items
```

## Filter

Filters messages based on JSONPath conditions. Messages that don't match the condition are removed from the stream.

### Configuration

```yaml
transformations:
  - type: filter
    filter:
      # JSONPath expression that must be true (required)
      condition: "$.active"
```

## Mask

Masks sensitive data in specified fields. Supports preserving length or full character replacement.

### Configuration

```yaml
transformations:
  - type: mask
    mask:
      # List of JSONPath expressions to fields to mask (required)
      fields:
        - password
        - email
      # Character for masking (optional, default: *)
      maskChar: "*"
      # Preserve original length (optional, default: false)
      keepLength: true
```

## Router

Routes messages to different sinks based on conditions. Messages matching a condition are sent to the specified sink instead of the main one.

### Configuration

```yaml
transformations:
  - type: router
    router:
      routes:
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: error-topic
```

## Select

Selects only specified fields from a message. Useful for reducing data size and improving performance.

### Configuration

```yaml
transformations:
  - type: select
    select:
      # List of JSONPath expressions to fields to select (required)
      fields:
        - id
        - name
        - email
```

## Remove

Removes specified fields from a message. Useful for data cleanup before sending.

### Configuration

```yaml
transformations:
  - type: remove
    remove:
      # List of JSONPath expressions to fields to remove (required)
      fields:
        - password
        - internal_id
```

## Order of Application

Transformations are applied sequentially in the order specified in the `transformations` list. Each transformation receives the result of the previous one.

### Recommended Order

1. **Flatten** should be first if you need to expand arrays
2. **Filter** apply early to reduce the volume of processed data
3. **SnakeCase/CamelCase** apply after Select/Remove, but before sending to sink
4. **Mask/Remove** apply before Select for security
5. **Select** apply at the end for final cleanup
6. **Timestamp** can be applied anywhere, but usually at the beginning or end
7. **Router** usually applied at the end, after all other transformations

## SnakeCase

Converts all JSON object keys to `snake_case` format. Useful for normalizing field names when integrating with systems using snake_case (e.g., PostgreSQL, Python API).

### Configuration

```yaml
transformations:
  - type: snakeCase
    snakeCase:
      # Recursively convert nested objects (optional, default: false)
      deep: true
```

### Example

**Input:**
```json
{
  "firstName": "John",
  "lastName": "Doe",
  "isActive": true
}
```

**Output:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "is_active": true
}
```

## CamelCase

Converts all JSON object keys to `CamelCase` (PascalCase) format. Useful for normalizing field names when integrating with systems using CamelCase (e.g., Java, C# API).

### Configuration

```yaml
transformations:
  - type: camelCase
    camelCase:
      # Recursively convert nested objects (optional, default: false)
      deep: true
```

### Example

**Input:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "is_active": true
}
```

**Output:**
```json
{
  "FirstName": "John",
  "LastName": "Doe",
  "IsActive": true
}
```

## ReplaceField

Renames fields in messages. Useful for normalizing data structure and changing field paths.

### Configuration

```yaml
transformations:
  - type: replaceField
    replaceField:
      # List of rename rules in format "old.path:new.path" (required)
      renames:
        - key.sku:sku
        - body.lc:lc
```

### Rename Rule Format

Rename rules have the format `old.path:new.path`:
- Left part (before `:`) - JSONPath to existing field
- Right part (after `:`) - JSONPath to new field
- JSONPath syntax is supported (you can use `$.` prefix)

### Example

**Input:**
```json
{
  "key": {
    "sku": "12345"
  },
  "body": {
    "lc": "en"
  }
}
```

**Output:**
```json
{
  "sku": "12345",
  "lc": "en"
}
```

## HeaderFrom

Extracts data from Kafka message headers and adds them to the message body. Useful for enriching messages with metadata from headers.

### Configuration

```yaml
transformations:
  - type: headerFrom
    headerFrom:
      # List of mappings in format "headerName:field.path" (required)
      mappings:
        - X-Request-Id:requestId
        - X-Language:metadata.language
```

### Mapping Format

Mappings have the format `headerName:field.path`:
- Left part (before `:`) - Kafka message header name
- Right part (after `:`) - JSONPath to field in message body where header value will be written
- JSONPath syntax is supported (you can use `$.` prefix)

### Example

**Input message (with header `X-Request-Id: req-123`):**
```json
{
  "data": "value"
}
```

**Output:**
```json
{
  "data": "value",
  "requestId": "req-123"
}
```

### Notes

- Headers are only available for messages from Kafka sources
- If a header doesn't exist, the mapping is skipped
- Header values are always added as strings

For complete transformation documentation with examples, see the [Russian version](../ru/transformations.md).

