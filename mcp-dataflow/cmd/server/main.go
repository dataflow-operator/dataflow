package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/server"
	"github.com/modelcontextprotocol/go-sdk/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/dataflow-operator/mcp-dataflow/internal/handlers"
	k8sclient "github.com/dataflow-operator/mcp-dataflow/internal/kubernetes"
	"k8s.io/client-go/dynamic"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig file")
	flag.Parse()

	// Инициализация Kubernetes клиента
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// Создание динамического клиента для работы с CRD
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating dynamic client: %v", err)
	}

	// Создание MCP сервера
	mcpServer, err := server.NewServer(
		server.WithName("dataflow"),
		server.WithVersion("1.0.0"),
	)
	if err != nil {
		log.Fatalf("Error creating MCP server: %v", err)
	}

	// Инициализация обработчиков
	dataflowHandler := handlers.NewDataFlowHandler(dynamicClient, clientset)
	configHandler := handlers.NewConfigHandler()
	metricsHandler := handlers.NewMetricsHandler(clientset)

	// Регистрация инструментов
	registerTools(mcpServer, dataflowHandler, configHandler, metricsHandler)

	// Запуск сервера
	if err := mcpServer.Run(context.Background()); err != nil {
		log.Fatalf("Error running MCP server: %v", err)
	}
}

func registerTools(
	mcpServer *server.Server,
	dataflowHandler *handlers.DataFlowHandler,
	configHandler *handlers.ConfigHandler,
	metricsHandler *handlers.MetricsHandler,
) {
	// Управление DataFlow ресурсами
	mcpServer.AddTool("create_dataflow", &types.Tool{
		Name:        "create_dataflow",
		Description: "Создает новый DataFlow ресурс в Kubernetes кластере",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"description": "Имя DataFlow ресурса",
				},
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace для создания ресурса (по умолчанию: default)",
					"default":     "default",
				},
				"config": map[string]interface{}{
					"type":        "string",
					"description": "YAML конфигурация DataFlow ресурса",
				},
			},
			"required": []string{"name", "config"},
		},
	}, dataflowHandler.CreateDataFlow)

	mcpServer.AddTool("get_dataflow", &types.Tool{
		Name:        "get_dataflow",
		Description: "Получает информацию о DataFlow ресурсе",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"description": "Имя DataFlow ресурса",
				},
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace ресурса (по умолчанию: default)",
					"default":     "default",
				},
			},
			"required": []string{"name"},
		},
	}, dataflowHandler.GetDataFlow)

	mcpServer.AddTool("list_dataflows", &types.Tool{
		Name:        "list_dataflows",
		Description: "Получает список всех DataFlow ресурсов",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace для фильтрации (опционально, если не указан - все namespaces)",
				},
			},
		},
	}, dataflowHandler.ListDataFlows)

	mcpServer.AddTool("delete_dataflow", &types.Tool{
		Name:        "delete_dataflow",
		Description: "Удаляет DataFlow ресурс",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"description": "Имя DataFlow ресурса",
				},
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace ресурса (по умолчанию: default)",
					"default":     "default",
				},
			},
			"required": []string{"name"},
		},
	}, dataflowHandler.DeleteDataFlow)

	// Генерация конфигураций
	mcpServer.AddTool("generate_dataflow_config", &types.Tool{
		Name:        "generate_dataflow_config",
		Description: "Генерирует YAML конфигурацию DataFlow на основе описания",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"description": map[string]interface{}{
					"type":        "string",
					"description": "Описание потока данных (например: 'Kafka to PostgreSQL с фильтрацией ошибок')",
				},
				"source_type": map[string]interface{}{
					"type":        "string",
					"description": "Тип источника: kafka, postgresql, trino",
					"enum":        []string{"kafka", "postgresql", "trino"},
				},
				"sink_type": map[string]interface{}{
					"type":        "string",
					"description": "Тип приемника: kafka, postgresql, trino",
					"enum":        []string{"kafka", "postgresql", "trino"},
				},
				"source_config": map[string]interface{}{
					"type":        "object",
					"description": "Конфигурация источника",
				},
				"sink_config": map[string]interface{}{
					"type":        "object",
					"description": "Конфигурация приемника",
				},
				"transformations": map[string]interface{}{
					"type":        "array",
					"description": "Список трансформаций",
					"items": map[string]interface{}{
						"type": "object",
					},
				},
			},
			"required": []string{"description", "source_type", "sink_type"},
		},
	}, configHandler.GenerateConfig)

	mcpServer.AddTool("validate_dataflow_config", &types.Tool{
		Name:        "validate_dataflow_config",
		Description: "Валидирует YAML конфигурацию DataFlow",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"config": map[string]interface{}{
					"type":        "string",
					"description": "YAML конфигурация для валидации",
				},
			},
			"required": []string{"config"},
		},
	}, configHandler.ValidateConfig)

	// Метрики и статус
	mcpServer.AddTool("get_dataflow_status", &types.Tool{
		Name:        "get_dataflow_status",
		Description: "Получает детальный статус DataFlow ресурса",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"description": "Имя DataFlow ресурса",
				},
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace ресурса (по умолчанию: default)",
					"default":     "default",
				},
			},
			"required": []string{"name"},
		},
	}, dataflowHandler.GetStatus)

	mcpServer.AddTool("get_dataflow_metrics", &types.Tool{
		Name:        "get_dataflow_metrics",
		Description: "Получает метрики Prometheus для DataFlow ресурса",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"description": "Имя DataFlow ресурса",
				},
				"namespace": map[string]interface{}{
					"type":        "string",
					"description": "Namespace ресурса (по умолчанию: default)",
					"default":     "default",
				},
			},
			"required": []string{"name"},
		},
	}, metricsHandler.GetMetrics)

	// Справочная информация
	mcpServer.AddTool("list_transformations", &types.Tool{
		Name:        "list_transformations",
		Description: "Получает список доступных трансформаций с описаниями",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{},
		},
	}, configHandler.ListTransformations)

	mcpServer.AddTool("list_connectors", &types.Tool{
		Name:        "list_connectors",
		Description: "Получает список поддерживаемых коннекторов",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{},
		},
	}, configHandler.ListConnectors)
}
