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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/retry"
)

const (
	icebergRESTSnapshotConflictToken = "snapshot id changed"
	icebergRESTPreflightTimeout      = 5 * time.Second
	maxIcebergRESTPreflightBodyBytes = 4096
	defaultIcebergRESTOAuth2Scope    = "catalog"
)

// icebergRESTAuthConfig holds REST catalog authentication settings shared by Nessie and Iceberg connectors.
type icebergRESTAuthConfig struct {
	authType           v1.NessieAuthenticationType
	bearerToken        string
	basicAuth          *v1.BasicAuthConfig
	oauth2ServerURI    string
	oauth2ClientID     string
	oauth2ClientSecret string
	oauth2Scope        string
}

func resolveIcebergRESTAuthentication(authType v1.NessieAuthenticationType, bearerToken string, basicAuth *v1.BasicAuthConfig) (token string, basic string) {
	mode := strings.ToUpper(strings.TrimSpace(string(authType)))
	if mode == "" {
		mode = string(v1.NessieAuthenticationAuto)
	}
	switch mode {
	case string(v1.NessieAuthenticationNone):
		return "", ""
	case string(v1.NessieAuthenticationBearer):
		return bearerToken, ""
	case string(v1.NessieAuthenticationBasic):
		if basicAuth != nil && basicAuth.Username != "" && basicAuth.Password != "" {
			return "", "Basic " + base64.StdEncoding.EncodeToString([]byte(basicAuth.Username+":"+basicAuth.Password))
		}
		return "", ""
	case string(v1.NessieAuthenticationAuto):
		fallthrough
	default:
		if bearerToken != "" {
			return bearerToken, ""
		}
		if basicAuth != nil && basicAuth.Username != "" && basicAuth.Password != "" {
			return "", "Basic " + base64.StdEncoding.EncodeToString([]byte(basicAuth.Username+":"+basicAuth.Password))
		}
		return "", ""
	}
}

func icebergRESTAuthOptions(cfg icebergRESTAuthConfig) ([]rest.Option, error) {
	var opts []rest.Option
	token, basic := resolveIcebergRESTAuthentication(cfg.authType, cfg.bearerToken, cfg.basicAuth)
	if token != "" {
		opts = append(opts, rest.WithOAuthToken(token))
	} else if cfg.oauth2ClientID != "" || cfg.oauth2ClientSecret != "" {
		cred := cfg.oauth2ClientID + ":" + cfg.oauth2ClientSecret
		opts = append(opts, rest.WithCredential(cred))
		scope := strings.TrimSpace(cfg.oauth2Scope)
		if scope == "" {
			scope = defaultIcebergRESTOAuth2Scope
		}
		opts = append(opts, rest.WithScope(scope))
		if uri := strings.TrimSpace(cfg.oauth2ServerURI); uri != "" {
			parsed, err := url.Parse(uri)
			if err != nil {
				return nil, fmt.Errorf("invalid oauth2ServerURI %q: %w", uri, err)
			}
			opts = append(opts, rest.WithAuthURI(parsed))
		}
	}
	if basic != "" {
		opts = append(opts, rest.WithCustomTransport(&basicAuthTransport{base: http.DefaultTransport, auth: basic}))
	}
	return opts, nil
}

func buildIcebergRESTCatalogOptions(cfg icebergRESTAuthConfig, warehouse, prefix string) ([]rest.Option, error) {
	opts, err := icebergRESTAuthOptions(cfg)
	if err != nil {
		return nil, err
	}
	if warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(warehouse))
	}
	if prefix := strings.TrimSpace(prefix); prefix != "" {
		opts = append(opts, rest.WithPrefix(prefix))
	}
	return opts, nil
}

func normalizeCatalogURI(catalogURI string) (string, error) {
	uri := strings.TrimSuffix(strings.TrimSpace(catalogURI), "/")
	if uri == "" {
		return "", fmt.Errorf("catalogURI is empty")
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("invalid catalogURI %q: %w", catalogURI, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("catalogURI must include scheme and host, got %q", catalogURI)
	}
	return uri, nil
}

func icebergRESTConfigURL(catalogURI, warehouse string) (string, error) {
	uri, err := normalizeCatalogURI(catalogURI)
	if err != nil {
		return "", err
	}
	configURL := uri + "/v1/config"
	if warehouse != "" {
		configURL += "?warehouse=" + url.QueryEscape(warehouse)
	}
	return configURL, nil
}

func runIcebergRESTPreflight(ctx context.Context, catalogURI, warehouse string, cfg icebergRESTAuthConfig) error {
	configURL, err := icebergRESTConfigURL(catalogURI, warehouse)
	if err != nil {
		return fmt.Errorf("iceberg REST preflight: %w", err)
	}
	preflightCtx, cancel := context.WithTimeout(ctx, icebergRESTPreflightTimeout)
	defer cancel()
	return icebergRESTPreflightRequest(preflightCtx, &http.Client{}, configURL, cfg, "catalog config")
}

func icebergRESTPreflightRequest(ctx context.Context, client *http.Client, endpoint string, cfg icebergRESTAuthConfig, what string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("iceberg REST preflight: failed to prepare %s request: %w", what, err)
	}
	token, basic := resolveIcebergRESTAuthentication(cfg.authType, cfg.bearerToken, cfg.basicAuth)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if basic != "" {
		req.Header.Set("Authorization", basic)
	}

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("iceberg REST preflight: timeout while checking %s at %s", what, endpoint)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("iceberg REST preflight: context canceled while checking %s at %s: %w", what, endpoint, ctx.Err())
		}
		return fmt.Errorf("iceberg REST preflight: failed to reach %s at %s: %w", what, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxIcebergRESTPreflightBodyBytes))
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return fmt.Errorf("iceberg REST preflight: %s check failed (%s): %s", what, endpoint, msg)
	}
	return nil
}

func newIcebergRESTCatalog(ctx context.Context, catalogName, catalogURI, warehouse, prefix string, cfg icebergRESTAuthConfig) (*rest.Catalog, error) {
	uri, err := normalizeCatalogURI(catalogURI)
	if err != nil {
		return nil, err
	}
	opts, err := buildIcebergRESTCatalogOptions(cfg, warehouse, prefix)
	if err != nil {
		return nil, err
	}
	cat, err := rest.NewCatalog(ctx, catalogName, uri, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog client: %w", err)
	}
	return cat, nil
}

// basicAuthTransport adds Authorization header to outgoing requests.
type basicAuthTransport struct {
	base http.RoundTripper
	auth string
}

func (t *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2.Header.Set("Authorization", t.auth)
	if t.base != nil {
		return t.base.RoundTrip(req2)
	}
	return http.DefaultTransport.RoundTrip(req2)
}

func icebergRESTMetaColumnTypesFromTable(tbl *table.Table, metaColumns []string) map[string]iceberg.Type {
	types := make(map[string]iceberg.Type, len(metaColumns))
	if tbl == nil || tbl.Schema() == nil {
		for _, col := range metaColumns {
			types[col] = iceberg.PrimitiveTypes.String
		}
		return types
	}
	schema := tbl.Schema()
	for _, col := range metaColumns {
		if f, ok := schema.FindFieldByName(col); ok {
			types[col] = f.Type
		} else {
			types[col] = iceberg.PrimitiveTypes.String
		}
	}
	return types
}

func icebergRESTDefaultSchema(rawMode bool) *iceberg.Schema {
	if rawMode {
		return iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 2, Name: "_metadata", Type: iceberg.PrimitiveTypes.String, Required: false},
		)
	}
	return iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false})
}

func validateIcebergRESTRawModeSchema(tbl *table.Table) error {
	if tbl == nil {
		return fmt.Errorf("table is nil")
	}
	schema := tbl.Schema()
	if schema == nil {
		return fmt.Errorf("table schema is nil")
	}
	if _, ok := schema.FindFieldByNameCaseInsensitive("data"); !ok {
		return fmt.Errorf("rawMode requires a \"data\" column in the Iceberg table")
	}
	if _, ok := schema.FindFieldByNameCaseInsensitive("_metadata"); !ok {
		return fmt.Errorf("rawMode requires a \"_metadata\" column in the Iceberg table")
	}
	return nil
}

func isRetryableIcebergRESTSnapshotConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), icebergRESTSnapshotConflictToken)
}

func isRetryableIcebergRESTAppendError(err error) bool {
	if err == nil {
		return false
	}
	if retry.IsTimeoutError(err) || retry.IsRetryableTransient(err) || isRetryableIcebergRESTSnapshotConflict(err) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, ": eof") || strings.Contains(lower, "context canceled")
}

// nessie wrappers for backward compatibility
func resolveNessieAuthentication(authType v1.NessieAuthenticationType, bearerToken string, basicAuth *v1.BasicAuthConfig) (token string, basic string) {
	return resolveIcebergRESTAuthentication(authType, bearerToken, basicAuth)
}

func nessieAuthOptions(authType v1.NessieAuthenticationType, bearerToken string, basicAuth *v1.BasicAuthConfig) []rest.Option {
	opts, err := icebergRESTAuthOptions(icebergRESTAuthConfig{
		authType: authType, bearerToken: bearerToken, basicAuth: basicAuth,
	})
	if err != nil {
		return nil
	}
	return opts
}

func nessieMetaColumnTypesFromTable(tbl *table.Table, metaColumns []string) map[string]iceberg.Type {
	return icebergRESTMetaColumnTypesFromTable(tbl, metaColumns)
}

func nessieIcebergSchema(rawMode bool) *iceberg.Schema {
	return icebergRESTDefaultSchema(rawMode)
}

func validateNessieRawModeSchema(tbl *table.Table) error {
	return validateIcebergRESTRawModeSchema(tbl)
}

func isRetryableNessieSnapshotConflict(err error) bool {
	return isRetryableIcebergRESTSnapshotConflict(err)
}

func isRetryableNessieAppendError(err error) bool {
	return isRetryableIcebergRESTAppendError(err)
}

func icebergRESTAuthFromSource(cfg *v1.IcebergSourceSpec) icebergRESTAuthConfig {
	if cfg == nil {
		return icebergRESTAuthConfig{}
	}
	return icebergRESTAuthConfig{
		authType:           cfg.AuthenticationType,
		bearerToken:        cfg.BearerToken,
		basicAuth:          cfg.BasicAuth,
		oauth2ServerURI:    cfg.OAuth2ServerURI,
		oauth2ClientID:     cfg.OAuth2ClientID,
		oauth2ClientSecret: cfg.OAuth2ClientSecret,
		oauth2Scope:        cfg.OAuth2Scope,
	}
}

func icebergRESTAuthFromSink(cfg *v1.IcebergSinkSpec) icebergRESTAuthConfig {
	if cfg == nil {
		return icebergRESTAuthConfig{}
	}
	return icebergRESTAuthConfig{
		authType:           cfg.AuthenticationType,
		bearerToken:        cfg.BearerToken,
		basicAuth:          cfg.BasicAuth,
		oauth2ServerURI:    cfg.OAuth2ServerURI,
		oauth2ClientID:     cfg.OAuth2ClientID,
		oauth2ClientSecret: cfg.OAuth2ClientSecret,
		oauth2Scope:        cfg.OAuth2Scope,
	}
}
