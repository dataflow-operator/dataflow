package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// DataFlowHandler обрабатывает операции с DataFlow ресурсами
type DataFlowHandler struct {
	dynamicClient dynamic.Interface
	clientset     kubernetes.Interface
}

// NewDataFlowHandler создает новый обработчик DataFlow
func NewDataFlowHandler(dynamicClient dynamic.Interface, clientset kubernetes.Interface) *DataFlowHandler {
	return &DataFlowHandler{
		dynamicClient: dynamicClient,
		clientset:     clientset,
	}
}

// DataFlow GVR (Group Version Resource)
var dataflowGVR = schema.GroupVersionResource{
	Group:    "dataflow.dataflow.io",
	Version:  "v1",
	Resource: "dataflows",
}

// CreateDataFlow создает новый DataFlow ресурс
func (h *DataFlowHandler) CreateDataFlow(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	name, ok := args["name"].(string)
	if !ok {
		return nil, fmt.Errorf("name is required")
	}

	namespace := "default"
	if ns, ok := args["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	configYAML, ok := args["config"].(string)
	if !ok {
		return nil, fmt.Errorf("config is required")
	}

	// Парсинг YAML конфигурации
	var obj unstructured.Unstructured
	if err := json.Unmarshal([]byte(configYAML), &obj.Object); err != nil {
		// Попробуем распарсить как YAML
		// TODO: добавить YAML парсер
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Установка имени и namespace
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "dataflow.dataflow.io",
		Version: "v1",
		Kind:    "DataFlow",
	})

	// Создание ресурса
	client := h.dynamicClient.Resource(dataflowGVR).Namespace(namespace)
	created, err := client.Create(ctx, &obj, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFlow: %w", err)
	}

	result, _ := json.MarshalIndent(created.Object, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: fmt.Sprintf("DataFlow '%s' успешно создан в namespace '%s':\n%s", name, namespace, string(result)),
			},
		},
	}, nil
}

// GetDataFlow получает информацию о DataFlow ресурсе
func (h *DataFlowHandler) GetDataFlow(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	name, ok := args["name"].(string)
	if !ok {
		return nil, fmt.Errorf("name is required")
	}

	namespace := "default"
	if ns, ok := args["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	client := h.dynamicClient.Resource(dataflowGVR).Namespace(namespace)
	obj, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get DataFlow: %w", err)
	}

	result, _ := json.MarshalIndent(obj.Object, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: string(result),
			},
		},
	}, nil
}

// ListDataFlows получает список всех DataFlow ресурсов
func (h *DataFlowHandler) ListDataFlows(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	namespace := ""
	if ns, ok := args["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	var client dynamic.ResourceInterface
	if namespace != "" {
		client = h.dynamicClient.Resource(dataflowGVR).Namespace(namespace)
	} else {
		client = h.dynamicClient.Resource(dataflowGVR)
	}

	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list DataFlows: %w", err)
	}

	result, _ := json.MarshalIndent(list.Items, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: string(result),
			},
		},
	}, nil
}

// DeleteDataFlow удаляет DataFlow ресурс
func (h *DataFlowHandler) DeleteDataFlow(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	name, ok := args["name"].(string)
	if !ok {
		return nil, fmt.Errorf("name is required")
	}

	namespace := "default"
	if ns, ok := args["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	client := h.dynamicClient.Resource(dataflowGVR).Namespace(namespace)
	err := client.Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to delete DataFlow: %w", err)
	}

	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: fmt.Sprintf("DataFlow '%s' успешно удален из namespace '%s'", name, namespace),
			},
		},
	}, nil
}

// GetStatus получает детальный статус DataFlow ресурса
func (h *DataFlowHandler) GetStatus(ctx context.Context, args map[string]interface{}) (*types.CallToolResult, error) {
	name, ok := args["name"].(string)
	if !ok {
		return nil, fmt.Errorf("name is required")
	}

	namespace := "default"
	if ns, ok := args["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	client := h.dynamicClient.Resource(dataflowGVR).Namespace(namespace)
	obj, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get DataFlow: %w", err)
	}

	// Извлечение статуса
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil || !found {
		return &types.CallToolResult{
			Content: []types.Content{
				{
					Type: "text",
					Text: fmt.Sprintf("DataFlow '%s' не имеет статуса или еще не обработан", name),
				},
			},
		}, nil
	}

	result, _ := json.MarshalIndent(status, "", "  ")
	return &types.CallToolResult{
		Content: []types.Content{
			{
				Type: "text",
				Text: fmt.Sprintf("Статус DataFlow '%s':\n%s", name, string(result)),
			},
		},
	}, nil
}
