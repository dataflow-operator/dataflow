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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretResolver_ResolveSecretValue(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("test-user"),
			"password": []byte("test-password"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	// Test successful resolution
	ref := &dataflowv1.SecretRef{
		Name:      "test-secret",
		Namespace: "default",
		Key:       "username",
	}

	value, err := resolver.ResolveSecretValue(ctx, "default", ref)
	require.NoError(t, err)
	assert.Equal(t, "test-user", value)
}

func TestSecretResolver_ResolveSecretValue_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	ref := &dataflowv1.SecretRef{
		Name:      "non-existent",
		Namespace: "default",
		Key:       "username",
	}

	_, err := resolver.ResolveSecretValue(ctx, "default", ref)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get secret")
}

func TestSecretResolver_ResolveSecretValue_KeyNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"username": []byte("test-user"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver := NewSecretResolver(fakeClient)

	ctx := context.Background()

	ref := &dataflowv1.SecretRef{
		Name:      "test-secret",
		Namespace: "default",
		Key:       "password",
	}

	_, err := resolver.ResolveSecretValue(ctx, "default", ref)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key password not found")
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
