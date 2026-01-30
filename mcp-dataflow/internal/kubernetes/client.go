package kubernetes

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Client обертка для Kubernetes клиентов
type Client struct {
	Dynamic  dynamic.Interface
	Standard kubernetes.Interface
	Config   *rest.Config
}

// NewDynamicClient создает динамический клиент для работы с CRD
func NewDynamicClient(config *rest.Config) (dynamic.Interface, error) {
	return dynamic.NewForConfig(config)
}
