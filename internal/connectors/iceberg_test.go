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

package connectors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCatalogURI(t *testing.T) {
	t.Run("valid https", func(t *testing.T) {
		uri, err := normalizeCatalogURI("https://catalog.example.com/")
		require.NoError(t, err)
		assert.Equal(t, "https://catalog.example.com", uri)
	})

	t.Run("missing scheme", func(t *testing.T) {
		_, err := normalizeCatalogURI("catalog.example.com")
		require.Error(t, err)
	})

	t.Run("empty", func(t *testing.T) {
		_, err := normalizeCatalogURI("  ")
		require.Error(t, err)
	})
}

func TestIcebergRESTConfigURL(t *testing.T) {
	url, err := icebergRESTConfigURL("https://catalog:8181", "wh1")
	require.NoError(t, err)
	assert.Equal(t, "https://catalog:8181/v1/config?warehouse=wh1", url)

	url, err = icebergRESTConfigURL("https://catalog:8181", "")
	require.NoError(t, err)
	assert.Equal(t, "https://catalog:8181/v1/config", url)
}

func TestResolveIcebergRESTAuthentication(t *testing.T) {
	token, basic := resolveIcebergRESTAuthentication(v1.IcebergRESTAuthenticationBearer, "tok", nil)
	assert.Equal(t, "tok", token)
	assert.Empty(t, basic)

	_, basic = resolveIcebergRESTAuthentication(v1.IcebergRESTAuthenticationBasic, "", &v1.BasicAuthConfig{
		Username: "u",
		Password: "p",
	})
	assert.Contains(t, basic, "Basic ")
}

func TestRunIcebergRESTPreflight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config", r.URL.Path)
		assert.Equal(t, "Bearer my-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := runIcebergRESTPreflight(context.Background(), srv.URL, "", icebergRESTAuthConfig{
		authType:    v1.IcebergRESTAuthenticationBearer,
		bearerToken: "my-token",
	})
	require.NoError(t, err)
}

func TestIcebergRESTAuthOptionsOAuth2(t *testing.T) {
	opts, err := icebergRESTAuthOptions(icebergRESTAuthConfig{
		oauth2ClientID:     "client",
		oauth2ClientSecret: "secret",
		oauth2Scope:        "catalog",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, opts)
}

func TestIcebergRESTAuthFromSource(t *testing.T) {
	cfg := icebergRESTAuthFromSource(&v1.IcebergSourceSpec{
		BearerToken:        "t",
		OAuth2ClientID:     "id",
		OAuth2ClientSecret: "sec",
		OAuth2Scope:        "catalog",
	})
	assert.Equal(t, "t", cfg.bearerToken)
	assert.Equal(t, "id", cfg.oauth2ClientID)
}
