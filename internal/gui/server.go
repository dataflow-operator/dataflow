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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// Server представляет веб-сервер для GUI
type Server struct {
	bindAddr   string
	httpServer *http.Server
	client     client.Client
	k8sClient  *kubernetes.Clientset
	logger     logr.Logger
}

// NewServer создает новый GUI сервер
func NewServer(bindAddr, kubeconfig string) (*Server, error) {
	logger := log.Log.WithName("gui-server")

	// Настройка Kubernetes клиента
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			// Пробуем использовать kubeconfig из дефолтного места
			config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes config: %w", err)
	}

	// Создаем controller-runtime клиент
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(dataflowv1.AddToScheme(scheme))

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Создаем стандартный Kubernetes клиент для работы с логами
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	server := &Server{
		bindAddr:  bindAddr,
		client:    k8sClient,
		k8sClient: clientset,
		logger:    logger,
	}

	// Настройка маршрутов
	mux := http.NewServeMux()

	// API endpoints
	apiHandler := NewAPIHandler(server)
	mux.Handle("/api/", http.StripPrefix("/api", apiHandler))

	// Статические файлы (frontend)
	mux.Handle("/", http.FileServer(http.Dir("./web/static")))

	server.httpServer = &http.Server{
		Addr:         bindAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return server, nil
}

// Start запускает HTTP сервер
func (s *Server) Start() error {
	s.logger.Info("Starting GUI server", "address", s.bindAddr)
	return s.httpServer.ListenAndServe()
}

// Stop останавливает HTTP сервер
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping GUI server")
	return s.httpServer.Shutdown(ctx)
}
