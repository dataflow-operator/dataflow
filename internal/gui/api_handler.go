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

package gui

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// APIHandler обрабатывает API запросы
type APIHandler struct {
	server *Server
}

// NewAPIHandler создает новый API handler
func NewAPIHandler(server *Server) *APIHandler {
	return &APIHandler{server: server}
}

// ServeHTTP обрабатывает HTTP запросы
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Устанавливаем CORS заголовки
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Устанавливаем Content-Type
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	// Фильтруем пустые части
	filteredParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			filteredParts = append(filteredParts, part)
		}
	}

	if len(filteredParts) == 0 {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	switch {
	case filteredParts[0] == "dataflows":
		h.handleDataFlows(w, r, filteredParts[1:])
	case filteredParts[0] == "logs":
		h.handleLogs(w, r, filteredParts[1:])
	case filteredParts[0] == "metrics":
		h.handleMetrics(w, r, filteredParts[1:])
	case filteredParts[0] == "status":
		h.handleStatus(w, r, filteredParts[1:])
	case filteredParts[0] == "namespaces":
		h.handleNamespaces(w, r)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

// handleDataFlows обрабатывает запросы к DataFlow ресурсам
func (h *APIHandler) handleDataFlows(w http.ResponseWriter, r *http.Request, parts []string) {
	namespace := r.URL.Query().Get("namespace")
	if namespace == "" {
		namespace = "default"
	}

	switch r.Method {
	case "GET":
		if len(parts) == 0 {
			// Список всех DataFlow
			h.listDataFlows(w, r, namespace)
		} else {
			// Получить конкретный DataFlow
			h.getDataFlow(w, r, namespace, parts[0])
		}
	case "POST":
		// Создать новый DataFlow
		h.createDataFlow(w, r, namespace)
	case "PUT":
		if len(parts) > 0 {
			// Обновить DataFlow
			h.updateDataFlow(w, r, namespace, parts[0])
		} else {
			http.Error(w, "Name required", http.StatusBadRequest)
		}
	case "DELETE":
		if len(parts) > 0 {
			// Удалить DataFlow
			h.deleteDataFlow(w, r, namespace, parts[0])
		} else {
			http.Error(w, "Name required", http.StatusBadRequest)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listDataFlows возвращает список всех DataFlow
func (h *APIHandler) listDataFlows(w http.ResponseWriter, r *http.Request, namespace string) {
	var list dataflowv1.DataFlowList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}

	if err := h.server.client.List(r.Context(), &list, opts...); err != nil {
		h.server.logger.Error(err, "Failed to list DataFlows")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(list.Items)
}

// getDataFlow возвращает конкретный DataFlow
func (h *APIHandler) getDataFlow(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var df dataflowv1.DataFlow
	key := types.NamespacedName{Namespace: namespace, Name: name}

	if err := h.server.client.Get(r.Context(), key, &df); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		h.server.logger.Error(err, "Failed to get DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(df)
}

// createDataFlow создает новый DataFlow
func (h *APIHandler) createDataFlow(w http.ResponseWriter, r *http.Request, namespace string) {
	var df dataflowv1.DataFlow
	if err := json.NewDecoder(r.Body).Decode(&df); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	df.Namespace = namespace
	df.APIVersion = "dataflow.dataflow.io/v1"
	df.Kind = "DataFlow"

	if err := h.server.client.Create(r.Context(), &df); err != nil {
		h.server.logger.Error(err, "Failed to create DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(df)
}

// updateDataFlow обновляет существующий DataFlow
func (h *APIHandler) updateDataFlow(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var df dataflowv1.DataFlow
	key := types.NamespacedName{Namespace: namespace, Name: name}

	// Получаем текущий ресурс
	if err := h.server.client.Get(r.Context(), key, &df); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		h.server.logger.Error(err, "Failed to get DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Декодируем обновления
	var updates dataflowv1.DataFlow
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Обновляем spec
	df.Spec = updates.Spec

	if err := h.server.client.Update(r.Context(), &df); err != nil {
		h.server.logger.Error(err, "Failed to update DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(df)
}

// deleteDataFlow удаляет DataFlow
func (h *APIHandler) deleteDataFlow(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var df dataflowv1.DataFlow
	key := types.NamespacedName{Namespace: namespace, Name: name}

	if err := h.server.client.Get(r.Context(), key, &df); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		h.server.logger.Error(err, "Failed to get DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.server.client.Delete(r.Context(), &df); err != nil {
		h.server.logger.Error(err, "Failed to delete DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleLogs обрабатывает запросы к логам
func (h *APIHandler) handleLogs(w http.ResponseWriter, r *http.Request, parts []string) {
	namespace := r.URL.Query().Get("namespace")
	name := r.URL.Query().Get("name")
	tailLines := r.URL.Query().Get("tailLines")
	follow := r.URL.Query().Get("follow") == "true"

	if namespace == "" || name == "" {
		http.Error(w, "namespace and name required", http.StatusBadRequest)
		return
	}

	// Получаем поды для DataFlow
	pods, err := h.server.k8sClient.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("dataflow.dataflow.io/name=%s", name),
	})
	if err != nil {
		h.server.logger.Error(err, "Failed to list pods")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(pods.Items) == 0 {
		// Пробуем найти под по имени deployment
		podName := fmt.Sprintf("dataflow-%s", name)
		pod, err := h.server.k8sClient.CoreV1().Pods(namespace).Get(r.Context(), podName, metav1.GetOptions{})
		if err != nil {
			// Пробуем найти поды по deployment
			pods, err = h.server.k8sClient.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=dataflow-processor,dataflow.dataflow.io/name=%s", name),
			})
			if err != nil || len(pods.Items) == 0 {
				http.Error(w, "Pod not found", http.StatusNotFound)
				return
			}
		} else {
			pods.Items = []corev1.Pod{*pod}
		}
	}

	if len(pods.Items) == 0 {
		http.Error(w, "No pods found", http.StatusNotFound)
		return
	}

	// Получаем логи из первого пода
	pod := pods.Items[0]
	containerName := "processor"

	opts := &corev1.PodLogOptions{
		Container: containerName,
		Follow:    follow,
	}

	if tailLines != "" {
		if tail, err := strconv.ParseInt(tailLines, 10, 64); err == nil {
			opts.TailLines = &tail
		}
	} else {
		tail := int64(100)
		opts.TailLines = &tail
	}

	req := h.server.k8sClient.CoreV1().Pods(namespace).GetLogs(pod.Name, opts)
	logs, err := req.Stream(r.Context())
	if err != nil {
		h.server.logger.Error(err, "Failed to get logs")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer logs.Close()

	// Если follow=true, используем Server-Sent Events
	if follow {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		scanner := bufio.NewScanner(logs)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintf(w, "data: %s\n\n", line)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if err := scanner.Err(); err != nil && err != io.EOF {
			h.server.logger.Error(err, "Error reading logs")
		}
	} else {
		// Просто копируем логи
		io.Copy(w, logs)
	}
}

// handleMetrics обрабатывает запросы к метрикам
func (h *APIHandler) handleMetrics(w http.ResponseWriter, r *http.Request, parts []string) {
	namespace := r.URL.Query().Get("namespace")
	name := r.URL.Query().Get("name")

	if namespace == "" || name == "" {
		http.Error(w, "namespace and name required", http.StatusBadRequest)
		return
	}

	// Получаем метрики из Prometheus endpoint оператора
	// В реальной реализации нужно подключиться к Prometheus API
	metrics := map[string]interface{}{
		"namespace": namespace,
		"name":      name,
		"metrics":   map[string]interface{}{},
	}

	json.NewEncoder(w).Encode(metrics)
}

// handleStatus обрабатывает запросы к статусу
func (h *APIHandler) handleStatus(w http.ResponseWriter, r *http.Request, parts []string) {
	namespace := r.URL.Query().Get("namespace")
	name := r.URL.Query().Get("name")

	if namespace == "" || name == "" {
		http.Error(w, "namespace and name required", http.StatusBadRequest)
		return
	}

	var df dataflowv1.DataFlow
	key := types.NamespacedName{Namespace: namespace, Name: name}

	if err := h.server.client.Get(r.Context(), key, &df); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		h.server.logger.Error(err, "Failed to get DataFlow")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	status := map[string]interface{}{
		"phase":             df.Status.Phase,
		"message":           df.Status.Message,
		"processedCount":    df.Status.ProcessedCount,
		"errorCount":        df.Status.ErrorCount,
		"lastProcessedTime": df.Status.LastProcessedTime,
	}

	json.NewEncoder(w).Encode(status)
}

// handleNamespaces обрабатывает запросы к списку namespaces
func (h *APIHandler) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	namespaces, err := h.server.k8sClient.CoreV1().Namespaces().List(r.Context(), metav1.ListOptions{})
	if err != nil {
		h.server.logger.Error(err, "Failed to list namespaces")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Извлекаем только имена namespaces
	namespaceNames := make([]string, 0, len(namespaces.Items))
	for _, ns := range namespaces.Items {
		namespaceNames = append(namespaceNames, ns.Name)
	}

	json.NewEncoder(w).Encode(namespaceNames)
}
