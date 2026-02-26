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

package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretResolver_ResolveSecretValue(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secretWithBoth := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("test-user"),
			"password": []byte("test-password"),
		},
	}

	secretUsernameOnly := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "partial-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("test-user"),
		},
	}

	tests := []struct {
		name        string
		objects     []client.Object
		ref         *dataflowv1.SecretRef
		namespace   string
		wantValue   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "successful resolution",
			objects:   []client.Object{secretWithBoth},
			ref:       &dataflowv1.SecretRef{Name: "test-secret", Namespace: "default", Key: "username"},
			namespace: "default",
			wantValue: "test-user",
			wantErr:   false,
		},
		{
			name:        "secret not found",
			objects:     nil,
			ref:         &dataflowv1.SecretRef{Name: "non-existent", Namespace: "default", Key: "username"},
			namespace:   "default",
			wantErr:     true,
			errContains: "failed to get secret",
		},
		{
			name:        "key not found in secret",
			objects:     []client.Object{secretUsernameOnly},
			ref:         &dataflowv1.SecretRef{Name: "partial-secret", Namespace: "default", Key: "password"},
			namespace:   "default",
			wantErr:     true,
			errContains: "key password not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(tt.objects) > 0 {
				builder = builder.WithObjects(tt.objects...)
			}
			fakeClient := builder.Build()
			resolver := NewSecretResolver(fakeClient)
			ctx := context.Background()

			value, err := resolver.ResolveSecretValue(ctx, tt.namespace, tt.ref)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, value)
		})
	}
}

func TestSecretResolver_ResolveSASLConfig_WithDirectValues(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "test-user",
		Password:  "test-password",
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.NoError(t, err)
	assert.Equal(t, "test-user", config.Username)
	assert.Equal(t, "test-password", config.Password)
}

func TestSecretResolver_ResolveSASLConfig_WithSecretRefs(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sasl-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("secret-user"),
			"password": []byte("secret-password"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		UsernameSecretRef: &dataflowv1.SecretRef{
			Name:      "sasl-secret",
			Namespace: "default",
			Key:       "username",
		},
		PasswordSecretRef: &dataflowv1.SecretRef{
			Name:      "sasl-secret",
			Namespace: "default",
			Key:       "password",
		},
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.NoError(t, err)
	assert.Equal(t, "secret-user", config.Username)
	assert.Equal(t, "secret-password", config.Password)
}

func TestSecretResolver_ResolveSASLConfig_MissingUsername(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		// Username is missing
		Password: "test-password",
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SASL username is required")
	assert.Contains(t, err.Error(), "either 'username' or 'usernameSecretRef' must be specified")
}

func TestSecretResolver_ResolveSASLConfig_MissingPassword(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "test-user",
		// Password is missing
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SASL password is required")
	assert.Contains(t, err.Error(), "either 'password' or 'passwordSecretRef' must be specified")
}

func TestSecretResolver_ResolveSASLConfig_MissingBothUsernameAndPassword(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		// Both username and password are missing
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.Error(t, err)
	// Should fail on username first
	assert.Contains(t, err.Error(), "SASL username is required")
}

func TestSecretResolver_ResolveSASLConfig_MixedDirectAndSecretRef(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sasl-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("secret-password"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "direct-user", // Direct value
		PasswordSecretRef: &dataflowv1.SecretRef{
			Name:      "sasl-secret",
			Namespace: "default",
			Key:       "password",
		},
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.NoError(t, err)
	assert.Equal(t, "direct-user", config.Username)
	assert.Equal(t, "secret-password", config.Password)
}

func TestSecretResolver_ResolveSASLConfig_SecretRefOverridesDirectValue(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sasl-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("secret-user"),
			"password": []byte("secret-password"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "direct-user", // Will be overridden
		Password:  "direct-pass", // Will be overridden
		UsernameSecretRef: &dataflowv1.SecretRef{
			Name:      "sasl-secret",
			Namespace: "default",
			Key:       "username",
		},
		PasswordSecretRef: &dataflowv1.SecretRef{
			Name:      "sasl-secret",
			Namespace: "default",
			Key:       "password",
		},
	}

	err := resolver.resolveSASLConfig(ctx, "default", config)
	require.NoError(t, err)
	// SecretRef values should override direct values
	assert.Equal(t, "secret-user", config.Username)
	assert.Equal(t, "secret-password", config.Password)
}

func TestSecretResolver_ResolveTLSConfig_WithCertificateContent_CreatesTempFiles(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	certContent := `-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKs+RHX+O9sOMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yNDAxMDEwMDAwMDBaFw0yNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnRlc3RjYTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA
-----END CERTIFICATE-----`
	keyContent := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC
-----END PRIVATE KEY-----`
	caContent := `-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKs+RHX+O9sOMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yNDAxMDEwMDAwMDBaFw0yNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnRlc3RjYTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA
-----END CERTIFICATE-----`

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"cert": []byte(certContent),
			"key":  []byte(keyContent),
			"ca":   []byte(caContent),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)
	defer func() { _ = resolver.CleanupTempFiles() }()

	ctx := context.Background()

	config := &dataflowv1.TLSConfig{
		CertSecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "cert",
		},
		KeySecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "key",
		},
		CASecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "ca",
		},
	}

	err := resolver.resolveTLSConfig(ctx, "default", config)
	require.NoError(t, err)

	assert.NotEmpty(t, config.CertFile, "CertFile should be set to temp file path")
	assert.NotEmpty(t, config.KeyFile, "KeyFile should be set to temp file path")
	assert.NotEmpty(t, config.CAFile, "CAFile should be set to temp file path")

	// Verify temp files exist and contain correct content
	certData, err := os.ReadFile(config.CertFile)
	require.NoError(t, err)
	assert.Equal(t, certContent, string(certData))

	keyData, err := os.ReadFile(config.KeyFile)
	require.NoError(t, err)
	assert.Equal(t, keyContent, string(keyData))

	caData, err := os.ReadFile(config.CAFile)
	require.NoError(t, err)
	assert.Equal(t, caContent, string(caData))

	// Verify CA file is not empty (resolveTLSConfig checks this)
	caStat, err := os.Stat(config.CAFile)
	require.NoError(t, err)
	assert.Greater(t, caStat.Size(), int64(0), "CA file should not be empty")
}

func TestSecretResolver_ResolveTLSConfig_WithFilePath_UsesExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "cert.pem")
	keyPath := filepath.Join(tmpDir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, []byte("cert content"), 0600))
	require.NoError(t, os.WriteFile(keyPath, []byte("key content"), 0600))

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"cert": []byte(certPath),
			"key":  []byte(keyPath),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)
	defer func() { _ = resolver.CleanupTempFiles() }()

	ctx := context.Background()

	config := &dataflowv1.TLSConfig{
		CertSecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "cert",
		},
		KeySecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "key",
		},
	}

	err := resolver.resolveTLSConfig(ctx, "default", config)
	require.NoError(t, err)

	assert.Equal(t, certPath, config.CertFile)
	assert.Equal(t, keyPath, config.KeyFile)
}

func TestSecretResolver_ResolveTLSConfig_CleanupTempFiles(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"cert": []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	config := &dataflowv1.TLSConfig{
		CertSecretRef: &dataflowv1.SecretRef{
			Name:      "tls-secret",
			Namespace: "default",
			Key:       "cert",
		},
	}

	err := resolver.resolveTLSConfig(ctx, "default", config)
	require.NoError(t, err)

	tempFile := config.CertFile
	_, err = os.Stat(tempFile)
	require.NoError(t, err, "temp file should exist before cleanup")

	err = resolver.CleanupTempFiles()
	require.NoError(t, err)

	_, err = os.Stat(tempFile)
	require.Error(t, err, "temp file should be removed after cleanup")
	assert.True(t, os.IsNotExist(err))
}
