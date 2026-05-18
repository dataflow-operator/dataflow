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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/go-logr/logr"
)

var (
	errDMLComplete    = errors.New("trino DML complete")
	errPaginationDone = errors.New("trino pagination complete")
)

// trinoClient provides shared HTTP-based query execution for Trino source and sink connectors.
type trinoClient struct {
	serverURL    string
	catalog      string
	schema       string
	httpClient   *http.Client
	keycloakAuth *KeycloakAuth
	logger       logr.Logger
}

// trinoClientConfig holds connection parameters for creating a trinoClient.
type trinoClientConfig struct {
	ServerURL  string
	Catalog    string
	Schema     string
	Keycloak   *v1.KeycloakConfig
	HTTPClient *http.Client // optional, for tests; when set, used instead of default client
}

// newTrinoClient creates a trinoClient and optionally sets up Keycloak auth.
func newTrinoClient(ctx context.Context, cfg trinoClientConfig, logger logr.Logger) (*trinoClient, error) {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	c := &trinoClient{
		serverURL:  cfg.ServerURL,
		catalog:    cfg.Catalog,
		schema:     cfg.Schema,
		httpClient: httpClient,
		logger:     logger,
	}
	if cfg.Keycloak != nil {
		c.keycloakAuth = &KeycloakAuth{
			config:     cfg.Keycloak,
			httpClient: c.httpClient,
			logger:     logger,
		}
		if err := SetupKeycloakAuth(ctx, c.keycloakAuth); err != nil {
			return nil, fmt.Errorf("failed to setup Keycloak authentication: %w", err)
		}
	}
	return c, nil
}

// testConnection verifies the Trino connection with a simple query.
func (c *trinoClient) testConnection(ctx context.Context) error {
	_, err := c.executeQuery(ctx, "SELECT 1")
	return err
}

// formatTrinoError builds a descriptive error message from a TrinoError.
func formatTrinoError(te *TrinoError) string {
	if te == nil {
		return "Trino query failed"
	}
	msg := fmt.Sprintf("Trino query failed: %s", te.Message)
	if te.ErrorName != "" {
		msg = fmt.Sprintf("%s (Error: %s, Code: %d)", msg, te.ErrorName, te.ErrorCode)
	}
	if te.ErrorLocation != nil {
		msg = fmt.Sprintf("%s at line %d, column %d", msg, te.ErrorLocation.LineNumber, te.ErrorLocation.ColumnNumber)
	}
	if te.FailureInfo != nil {
		errorDetails, _ := json.Marshal(te.FailureInfo)
		msg = fmt.Sprintf("%s. Details: %s", msg, string(errorDetails))
	}
	return msg
}

func isDMLStatement(query string) bool {
	upper := strings.TrimSpace(strings.ToUpper(query))
	for _, prefix := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// executeQuery executes a SQL query on Trino and returns results as maps.
func (c *trinoClient) executeQuery(ctx context.Context, query string) ([]map[string]interface{}, error) {
	c.logger.Info("Executing SQL query on Trino", "query", query, "catalog", c.catalog, "schema", c.schema)

	queryURL := fmt.Sprintf("%s/v1/statement", strings.TrimSuffix(c.serverURL, "/"))

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Trino-User", "dataflow-operator")
	req.Header.Set("X-Trino-Catalog", c.catalog)
	req.Header.Set("X-Trino-Schema", c.schema)

	if c.keycloakAuth != nil {
		if token := c.keycloakAuth.GetToken(); token != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error(err, "Failed to execute Trino query", "query", query, "url", queryURL)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Error(nil, "Trino query failed", "query", query, "status", resp.StatusCode, "response", string(body))
		return nil, fmt.Errorf("Trino query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var queryResponse TrinoQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if queryResponse.Error != nil {
		errorMsg := formatTrinoError(queryResponse.Error)
		c.logger.Error(nil, "Trino query error in initial response", "query", query, "error", errorMsg)
		return nil, fmt.Errorf("%s", errorMsg)
	}

	if queryResponse.Stats.State == "FAILED" {
		errorMsg := formatTrinoError(queryResponse.Error)
		if queryResponse.Error == nil {
			responseBody, _ := json.Marshal(queryResponse)
			errorMsg = fmt.Sprintf("Trino query failed with state FAILED. Response: %s", string(responseBody))
		}
		c.logger.Error(nil, "Trino query failed in initial response", "query", query, "state", queryResponse.Stats.State)
		return nil, fmt.Errorf("%s", errorMsg)
	}

	isDML := isDMLStatement(query)

	allDataRows := make([][]interface{}, 0)
	if queryResponse.Data != nil {
		allDataRows = append(allDataRows, queryResponse.Data...)
	}

	nextURI := queryResponse.NextURI
	columns := queryResponse.Columns
	queryState := queryResponse.Stats.State

	for nextURI != "" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.logger.Info("Following next URI for Trino query", "nextURI", nextURI, "query", query, "state", queryState)

		nextResp, err := c.getNextURIWithRetry(ctx, nextURI)
		if err != nil {
			c.logger.Error(err, "Failed to follow next URI", "nextURI", nextURI, "query", query)
			return nil, fmt.Errorf("failed to follow next URI: %w", err)
		}

		pageErr := func() error {
			defer nextResp.Body.Close()

			if nextResp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(nextResp.Body)
				c.logger.Error(nil, "Trino next URI request failed", "nextURI", nextURI, "status", nextResp.StatusCode)
				return fmt.Errorf("Trino query failed with status %d: %s", nextResp.StatusCode, string(body))
			}

			var nextResponse TrinoQueryResponse
			if err := json.NewDecoder(nextResp.Body).Decode(&nextResponse); err != nil {
				c.logger.Error(err, "Failed to decode next response", "nextURI", nextURI)
				return fmt.Errorf("failed to decode next response: %w", err)
			}

			if nextResponse.Stats.State != "" {
				queryState = nextResponse.Stats.State
			}

			if nextResponse.Error != nil {
				errorMsg := formatTrinoError(nextResponse.Error)
				c.logger.Error(nil, "Trino query error", "query", query, "error", errorMsg)
				return fmt.Errorf("%s", errorMsg)
			}

			if queryState == "FAILED" {
				errorMsg := formatTrinoError(nextResponse.Error)
				if nextResponse.Error == nil {
					responseBody, _ := json.Marshal(nextResponse)
					errorMsg = fmt.Sprintf("Trino query failed with state FAILED. Response: %s", string(responseBody))
				}
				c.logger.Error(nil, "Trino query failed", "query", query, "state", queryState)
				return fmt.Errorf("%s", errorMsg)
			}

			if nextResponse.Data != nil {
				allDataRows = append(allDataRows, nextResponse.Data...)
			}

			if len(columns) == 0 && len(nextResponse.Columns) > 0 {
				columns = nextResponse.Columns
			}

			if isDML && (queryState == "FINISHED" || nextResponse.NextURI == "") {
				c.logger.Info("DML statement completed", "query", query, "state", queryState)
				return errDMLComplete
			}

			nextURI = nextResponse.NextURI

			if nextURI == "" && queryState == "FINISHED" {
				return errPaginationDone
			}
			return nil
		}()
		if pageErr == errDMLComplete {
			return []map[string]interface{}{}, nil
		}
		if pageErr == errPaginationDone {
			break
		}
		if pageErr != nil {
			return nil, pageErr
		}
	}

	if isDML {
		c.logger.Info("Trino DML statement executed successfully", "query", query, "state", queryState)
		return []map[string]interface{}{}, nil
	}

	results := make([]map[string]interface{}, 0, len(allDataRows))
	for _, row := range allDataRows {
		rowMap := make(map[string]interface{})
		for i, value := range row {
			if i < len(columns) {
				rowMap[columns[i].Name] = value
			} else {
				rowMap[fmt.Sprintf("col%d", i)] = value
			}
		}
		results = append(results, rowMap)
	}

	c.logger.Info("Trino query executed successfully", "query", query, "rowsReturned", len(results), "state", queryState)
	return results, nil
}

// getNextURI performs a GET on Trino's nextURI with the same context and auth as the initial statement POST.
func (c *trinoClient) getNextURI(ctx context.Context, nextURI string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURI, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create next URI request: %w", err)
	}
	if c.keycloakAuth != nil {
		if token := c.keycloakAuth.GetToken(); token != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}
	}
	return c.httpClient.Do(req)
}

// getNextURIWithRetry retries GET on the same nextURI when the failure looks transient (e.g. unexpected EOF from proxy).
func (c *trinoClient) getNextURIWithRetry(ctx context.Context, nextURI string) (*http.Response, error) {
	var resp *http.Response
	err := retry.OnRetry(ctx, retry.TrinoNextURIMaxAttempts, retry.TrinoNextURIInitialBackoff, retry.IsRetryableForTrino, func() error {
		var getErr error
		resp, getErr = c.getNextURI(ctx, nextURI)
		return getErr
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
