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
	"os"
	"path/filepath"
	"sync"
)

// FileStore persists checkpoints to a local JSON file.
// Each Save, Clear, and Flush writes the full map atomically.
type FileStore struct {
	path   string
	mu     sync.Mutex
	loaded bool
	data   map[string]json.RawMessage
}

// NewFileStore creates a checkpoint store backed by path.
// The file is created on the first write. Missing files load as empty.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("checkpoint file path is required")
	}
	return &FileStore{path: path}, nil
}

// Load returns checkpoint data for the given source type, or nil if none exists.
func (s *FileStore) Load(_ context.Context, sourceType string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoadedLocked(); err != nil {
		return nil, err
	}
	raw, ok := s.data[sourceType]
	if !ok || len(raw) == 0 {
		return nil, nil
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// Save persists checkpoint data for the given source type.
func (s *FileStore) Save(_ context.Context, sourceType string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoadedLocked(); err != nil {
		return err
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[sourceType] = json.RawMessage(cp)
	return s.writeLocked()
}

// Flush rewrites the checkpoint file from memory.
func (s *FileStore) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoadedLocked(); err != nil {
		return err
	}
	return s.writeLocked()
}

// Clear removes persisted checkpoint for the given source type.
func (s *FileStore) Clear(_ context.Context, sourceType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoadedLocked(); err != nil {
		return err
	}
	delete(s.data, sourceType)
	return s.writeLocked()
}

func (s *FileStore) ensureLoadedLocked() error {
	if s.loaded {
		return nil
	}
	s.loaded = true
	s.data = map[string]json.RawMessage{}
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint file: %w", err)
	}
	if len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, &s.data); err != nil {
		return fmt.Errorf("parse checkpoint file: %w", err)
	}
	if s.data == nil {
		s.data = map[string]json.RawMessage{}
	}
	return nil
}

func (s *FileStore) writeLocked() error {
	if s.data == nil {
		s.data = map[string]json.RawMessage{}
	}
	payload, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint file: %w", err)
	}
	payload = append(payload, '\n')
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".checkpoint-*.json")
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync checkpoint temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint temp file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		// Windows cannot replace an existing file via Rename.
		_ = os.Remove(s.path)
		if err2 := os.Rename(tmpName, s.path); err2 != nil {
			return fmt.Errorf("replace checkpoint file: %w", err2)
		}
	}
	ok = true
	return nil
}
