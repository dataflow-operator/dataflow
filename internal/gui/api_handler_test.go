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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

var ctx = context.Background()

func setupTestServer() (*Server, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(dataflowv1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Создаем минимальный сервер для тестирования
	server := &Server{
		client: fakeClient,
		logger: ctrl.Log.WithName("test"),
	}

	return server, nil
}

func TestAPIHandler_ListDataFlows(t *testing.T) {
	server, err := setupTestServer()
	if err != nil {
		t.Fatalf("Failed to setup test server: %v", err)
	}

	handler := NewAPIHandler(server)

	// Создаем тестовый DataFlow
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
			},
			Sink: dataflowv1.SinkSpec{
				Type: "postgresql",
			},
		},
	}

	if err := server.client.Create(ctx, df); err != nil {
		t.Fatalf("Failed to create test DataFlow: %v", err)
	}

	// Создаем запрос (путь уже без /api префикса, так как StripPrefix удаляет его)
	req := httptest.NewRequest("GET", "/dataflows?namespace=default", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var dataflows []dataflowv1.DataFlow
	if err := json.NewDecoder(w.Body).Decode(&dataflows); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(dataflows) != 1 {
		t.Errorf("Expected 1 DataFlow, got %d", len(dataflows))
	}

	if dataflows[0].Name != "test-dataflow" {
		t.Errorf("Expected name 'test-dataflow', got '%s'", dataflows[0].Name)
	}
}

func TestAPIHandler_GetDataFlow(t *testing.T) {
	server, err := setupTestServer()
	if err != nil {
		t.Fatalf("Failed to setup test server: %v", err)
	}

	handler := NewAPIHandler(server)

	// Создаем тестовый DataFlow
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
			},
			Sink: dataflowv1.SinkSpec{
				Type: "postgresql",
			},
		},
	}

	if err := server.client.Create(ctx, df); err != nil {
		t.Fatalf("Failed to create test DataFlow: %v", err)
	}

	// Создаем запрос (путь уже без /api префикса)
	req := httptest.NewRequest("GET", "/dataflows/test-dataflow?namespace=default", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var result dataflowv1.DataFlow
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result.Name != "test-dataflow" {
		t.Errorf("Expected name 'test-dataflow', got '%s'", result.Name)
	}
}

func TestAPIHandler_CreateDataFlow(t *testing.T) {
	server, err := setupTestServer()
	if err != nil {
		t.Fatalf("Failed to setup test server: %v", err)
	}

	handler := NewAPIHandler(server)

	// Создаем тестовый DataFlow для отправки
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name: "new-dataflow",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
			},
			Sink: dataflowv1.SinkSpec{
				Type: "postgresql",
			},
		},
	}

	body, _ := json.Marshal(df)
	req := httptest.NewRequest("POST", "/dataflows?namespace=default", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	// Проверяем, что DataFlow был создан
	var result dataflowv1.DataFlow
	if err := server.client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "new-dataflow"}, &result); err != nil {
		t.Fatalf("Failed to get created DataFlow: %v", err)
	}
}

func TestAPIHandler_DeleteDataFlow(t *testing.T) {
	server, err := setupTestServer()
	if err != nil {
		t.Fatalf("Failed to setup test server: %v", err)
	}

	handler := NewAPIHandler(server)

	// Создаем тестовый DataFlow
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
			},
			Sink: dataflowv1.SinkSpec{
				Type: "postgresql",
			},
		},
	}

	if err := server.client.Create(ctx, df); err != nil {
		t.Fatalf("Failed to create test DataFlow: %v", err)
	}

	// Создаем запрос на удаление (путь уже без /api префикса)
	req := httptest.NewRequest("DELETE", "/dataflows/test-dataflow?namespace=default", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status 204, got %d", w.Code)
	}

	// Проверяем, что DataFlow был удален
	var result dataflowv1.DataFlow
	if err := server.client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "test-dataflow"}, &result); err == nil {
		t.Error("Expected DataFlow to be deleted, but it still exists")
	}
}
