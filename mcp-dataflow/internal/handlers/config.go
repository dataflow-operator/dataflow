package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/types"
	"gopkg.in/yaml.v3"
)

// ConfigHandler обрабатывает генерацию и валидацию конфигураций
type ConfigHandler struct{}

// NewConfigHandler создает новый обработчик конфигураций
func NewConfigHandler() *ConfigHandler {
	return &ConfigHandler{}
}

// GenerateConfig генерирует YAML конфигурацию DataFlow
func (h *ConfigHandler) GenerateConfig(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	description, _ := args["description"].(string)
	sourceType, ok := args["source_type"].(string)
	if !ok {
		return nil, fmt.Errorf("source_type is required")
	}

	sinkType, ok := args["sink_type"].(string)
	if !ok {
		return nil, fmt.Errorf("sink_type is required")
	}

	// Создание базовой структуры
	config := map[string]interface{}{
		"apiVersion": "dataflow.dataflow.io/v1",
		"kind":       "DataFlow",
		"metadata": map[string]interface{}{
			"name": "dataflow-example",
		},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{
				"type": sourceType,
			},
			"sink": map[string]interface{}{
				"type": sinkType,
			},
		},
	}

	// Добавление конфигурации источника
	if sourceConfig, ok := args["source_config"].(map[string]interface{}); ok {
		config["spec"].(map[string]interface{})["source"].(map[string]interface{})[sourceType] = sourceConfig
	}

	// Добавление конфигурации приемника
	if sinkConfig, ok := args["sink_config"].(map[string]interface{}); ok {
		config["spec"].(map[string]interface{})["sink"].(map[string]interface{})[sinkType] = sinkConfig
	}

	// Добавление трансформаций
	if transformations, ok := args["transformations"].([]interface{}); ok {
		config["spec"].(map[string]interface{})["transformations"] = transformations
	}

	// Конвертация в YAML
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	result := fmt.Sprintf("Сгенерированная конфигурация DataFlow:\n\n%s\n\nОписание: %s", string(yamlBytes), description)
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: result,
			},
		},
	}, nil
}

// ValidateConfig валидирует YAML конфигурацию
func (h *ConfigHandler) ValidateConfig(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	configYAML, ok := args["config"].(string)
	if !ok {
		return nil, fmt.Errorf("config is required")
	}

	// Парсинг YAML
	var config map[string]interface{}
	if err := yaml.Unmarshal([]byte(configYAML), &config); err != nil {
		return &types.CallToolResult{
			Content: []types.Content{
				{
					Type: "text",
					Text: fmt.Sprintf("Ошибка парсинга YAML: %v", err),
				},
			},
		}, nil
	}

	// Базовая валидация структуры
	errors := []string{}

	// Проверка apiVersion
	if apiVersion, ok := config["apiVersion"].(string); !ok || apiVersion != "dataflow.dataflow.io/v1" {
		errors = append(errors, "apiVersion должен быть 'dataflow.dataflow.io/v1'")
	}

	// Проверка kind
	if kind, ok := config["kind"].(string); !ok || kind != "DataFlow" {
		errors = append(errors, "kind должен быть 'DataFlow'")
	}

	// Проверка spec
	spec, ok := config["spec"].(map[string]interface{})
	if !ok {
		errors = append(errors, "spec обязателен")
	} else {
		// Проверка source
		if _, ok := spec["source"].(map[string]interface{}); !ok {
			errors = append(errors, "spec.source обязателен")
		}

		// Проверка sink
		if _, ok := spec["sink"].(map[string]interface{}); !ok {
			errors = append(errors, "spec.sink обязателен")
		}
	}

	if len(errors) > 0 {
		return &types.CallToolResult{
			Content: []types.Content{
				{
					Type: "text",
					Text: fmt.Sprintf("Ошибки валидации:\n- %s", strings.Join(errors, "\n- ")),
				},
			},
		}, nil
	}

	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: "Конфигурация валидна",
			},
		},
	}, nil
}

// ListTransformations возвращает список доступных трансформаций
func (h *ConfigHandler) ListTransformations(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	transformations := map[string]interface{}{
		"timestamp": map[string]interface{}{
			"description": "Добавляет временную метку к каждому сообщению",
			"example": map[string]interface{}{
				"type": "timestamp",
				"timestamp": map[string]interface{}{
					"fieldName": "created_at",
					"format":    "RFC3339",
				},
			},
		},
		"flatten": map[string]interface{}{
			"description": "Разворачивает массив в отдельные сообщения",
			"example": map[string]interface{}{
				"type": "flatten",
				"flatten": map[string]interface{}{
					"field": "$.items",
				},
			},
		},
		"filter": map[string]interface{}{
			"description": "Фильтрует сообщения по условию JSONPath",
			"example": map[string]interface{}{
				"type": "filter",
				"filter": map[string]interface{}{
					"condition": "$.level != 'error'",
				},
			},
		},
		"mask": map[string]interface{}{
			"description": "Маскирует чувствительные данные",
			"example": map[string]interface{}{
				"type": "mask",
				"mask": map[string]interface{}{
					"fields":     []string{"$.password", "$.token"},
					"maskChar":   "*",
					"keepLength": true,
				},
			},
		},
		"router": map[string]interface{}{
			"description": "Маршрутизирует сообщения в разные приемники",
			"example": map[string]interface{}{
				"type": "router",
				"router": map[string]interface{}{
					"routes": []map[string]interface{}{
						{
							"condition": "$.level == 'error'",
							"sink": map[string]interface{}{
								"type": "kafka",
								"kafka": map[string]interface{}{
									"brokers": []string{"localhost:9092"},
									"topic":   "errors",
								},
							},
						},
					},
				},
			},
		},
		"select": map[string]interface{}{
			"description": "Выбирает определенные поля из сообщения",
			"example": map[string]interface{}{
				"type": "select",
				"select": map[string]interface{}{
					"fields": []string{"$.id", "$.name", "$.timestamp"},
				},
			},
		},
		"remove": map[string]interface{}{
			"description": "Удаляет определенные поля из сообщения",
			"example": map[string]interface{}{
				"type": "remove",
				"remove": map[string]interface{}{
					"fields": []string{"$.password", "$.token"},
				},
			},
		},
		"snakeCase": map[string]interface{}{
			"description": "Конвертирует имена полей в snake_case",
			"example": map[string]interface{}{
				"type": "snakeCase",
				"snakeCase": map[string]interface{}{
					"deep": true,
				},
			},
		},
		"camelCase": map[string]interface{}{
			"description": "Конвертирует имена полей в CamelCase",
			"example": map[string]interface{}{
				"type": "camelCase",
				"camelCase": map[string]interface{}{
					"deep": true,
				},
			},
		},
	}

	result, _ := json.MarshalIndent(transformations, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: string(result),
			},
		},
	}, nil
}

// ListConnectors возвращает список поддерживаемых коннекторов
func (h *ConfigHandler) ListConnectors(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	connectors := map[string]interface{}{
		"sources": map[string]interface{}{
			"kafka": map[string]interface{}{
				"description": "Чтение сообщений из Kafka топиков",
				"required_fields": []string{"brokers", "topic"},
				"optional_fields": []string{"consumerGroup", "tls", "sasl", "format", "avroSchema"},
			},
			"postgresql": map[string]interface{}{
				"description": "Чтение данных из PostgreSQL таблиц",
				"required_fields": []string{"connectionString", "table"},
				"optional_fields": []string{"query", "pollInterval"},
			},
			"trino": map[string]interface{}{
				"description": "Чтение данных из Trino таблиц",
				"required_fields": []string{"serverURL", "catalog", "schema", "table"},
				"optional_fields": []string{"query", "pollInterval", "keycloak"},
			},
		},
		"sinks": map[string]interface{}{
			"kafka": map[string]interface{}{
				"description": "Запись сообщений в Kafka топики",
				"required_fields": []string{"brokers", "topic"},
				"optional_fields": []string{"tls", "sasl"},
			},
			"postgresql": map[string]interface{}{
				"description": "Запись данных в PostgreSQL таблицы",
				"required_fields": []string{"connectionString", "table"},
				"optional_fields": []string{"batchSize", "autoCreateTable", "upsertMode", "conflictKey"},
			},
			"trino": map[string]interface{}{
				"description": "Запись данных в Trino таблицы",
				"required_fields": []string{"serverURL", "catalog", "schema", "table"},
				"optional_fields": []string{"batchSize", "autoCreateTable", "keycloak"},
			},
		},
	}

	result, _ := json.MarshalIndent(connectors, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: string(result),
			},
		},
	}, nil
}
