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

package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// checkpointKey is the key in ConfigMap.Data for the checkpoint JSON
	checkpointKey = "checkpoint.json"
	// defaultSaveInterval is how often to persist checkpoint to ConfigMap when debouncing
	defaultSaveInterval = 30 * time.Second
)

// NoopStore is a no-op implementation for when checkpoint persistence is disabled.
type NoopStore struct{}

func (NoopStore) Load(context.Context, string) ([]byte, error) { return nil, nil }
func (NoopStore) Save(context.Context, string, []byte) error   { return nil }
func (NoopStore) Flush(context.Context) error                  { return nil }

// Store manages loading and saving of source checkpoints.
// Implementations may debounce or batch saves.
type Store interface {
	// Load returns checkpoint data for the given source type, or nil if none exists.
	Load(ctx context.Context, sourceType string) ([]byte, error)
	// Save persists checkpoint data for the given source type.
	Save(ctx context.Context, sourceType string, data []byte) error
	// Flush forces an immediate write of any pending checkpoint (e.g. on shutdown).
	Flush(ctx context.Context) error
}

// ConfigMapStore persists checkpoints to a Kubernetes ConfigMap.
// Saves are debounced to avoid excessive API calls.
type ConfigMapStore struct {
	client       kubernetes.Interface
	namespace    string
	name         string
	pending      map[string][]byte
	pendingMu    sync.Mutex
	saveInterval time.Duration
	lastSave     time.Time
	ticker       *time.Ticker
	stopCh       chan struct{}
	stopOnce     sync.Once
}

// ConfigMapStoreOption configures ConfigMapStore.
type ConfigMapStoreOption func(*ConfigMapStore)

// WithSaveInterval sets the debounce interval for saving to ConfigMap.
func WithSaveInterval(d time.Duration) ConfigMapStoreOption {
	return func(s *ConfigMapStore) {
		s.saveInterval = d
	}
}

// NewConfigMapStore creates a checkpoint store that persists to a ConfigMap.
// Uses in-cluster config when running in cluster, or kubeconfig from KUBECONFIG env for local dev.
func NewConfigMapStore(namespace, configMapName string, opts ...ConfigMapStoreOption) (*ConfigMapStore, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig for local development
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("failed to build kube config: %w", err)
		}
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	return NewConfigMapStoreWithClient(client, namespace, configMapName, opts...)
}

// NewConfigMapStoreWithClient creates a checkpoint store backed by the given Kubernetes client.
func NewConfigMapStoreWithClient(client kubernetes.Interface, namespace, configMapName string, opts ...ConfigMapStoreOption) (*ConfigMapStore, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	s := &ConfigMapStore{
		client:       client,
		namespace:    namespace,
		name:         configMapName,
		pending:      make(map[string][]byte),
		saveInterval: defaultSaveInterval,
		lastSave:     time.Time{},
		stopCh:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Start begins the debounced save goroutine.
func (s *ConfigMapStore) Start(ctx context.Context) {
	s.ticker = time.NewTicker(s.saveInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-s.ticker.C:
				_ = s.Flush(ctx)
			}
		}
	}()
}

// Stop stops the debounced save goroutine.
func (s *ConfigMapStore) Stop() {
	if s.ticker != nil {
		s.ticker.Stop()
		s.ticker = nil
	}
	s.stopOnce.Do(func() { close(s.stopCh) })
}

// Load returns checkpoint data for the given source type.
func (s *ConfigMapStore) Load(ctx context.Context, sourceType string) ([]byte, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get ConfigMap %s/%s: %w", s.namespace, s.name, err)
	}
	if cm.Data == nil {
		return nil, nil
	}
	all, ok := cm.Data[checkpointKey]
	if !ok || all == "" {
		return nil, nil
	}
	// Parse the JSON map and extract the source type key
	data, err := extractSourceCheckpoint(all, sourceType)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Save queues checkpoint data for persistence. Actual write happens on Flush or debounce.
func (s *ConfigMapStore) Save(ctx context.Context, sourceType string, data []byte) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pending[sourceType] = data
	return nil
}

// Flush writes all pending checkpoints to the ConfigMap.
func (s *ConfigMapStore) Flush(ctx context.Context) error {
	s.pendingMu.Lock()
	pending := make(map[string][]byte)
	for k, v := range s.pending {
		pending[k] = v
	}
	s.pendingMu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	// Merge pending into existing ConfigMap data
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// ConfigMap may not exist yet (controller creates it); skip flush
			return nil
		}
		return fmt.Errorf("failed to get ConfigMap for flush: %w", err)
	}

	merged := mergeCheckpointData(cm.Data, pending)

	dataPatch := map[string]string{checkpointKey: string(merged)}
	patchBytes, err := json.Marshal(map[string]interface{}{"data": dataPatch})
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}
	_, err = s.client.CoreV1().ConfigMaps(s.namespace).Patch(ctx, s.name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch ConfigMap: %w", err)
	}

	s.pendingMu.Lock()
	s.lastSave = time.Now()
	s.pending = make(map[string][]byte) // clear after successful write
	s.pendingMu.Unlock()

	return nil
}

// extractSourceCheckpoint parses the full checkpoint JSON and returns the bytes for the given source type.
func extractSourceCheckpoint(all string, sourceType string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(all), &m); err != nil {
		return nil, err
	}
	raw, ok := m[sourceType]
	if !ok {
		return nil, nil
	}
	return raw, nil
}

// mergeCheckpointData merges pending checkpoint updates into existing ConfigMap data.
func mergeCheckpointData(existing map[string]string, pending map[string][]byte) []byte {
	var combined map[string]json.RawMessage
	if existing != nil {
		if raw, ok := existing[checkpointKey]; ok && raw != "" {
			_ = json.Unmarshal([]byte(raw), &combined)
		}
	}
	if combined == nil {
		combined = make(map[string]json.RawMessage)
	}
	for sourceType, data := range pending {
		combined[sourceType] = json.RawMessage(data)
	}
	out, _ := json.Marshal(combined)
	return out
}
