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
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/go-logr/logr"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// KeycloakAuth handles OAuth2/Keycloak authentication for Trino connectors.
type KeycloakAuth struct {
	config      *v1.KeycloakConfig
	httpClient  *http.Client
	token       string
	tokenMu     sync.RWMutex
	logger      logr.Logger
	tokenSource oauth2.TokenSource
}

// SetupKeycloakAuth configures OAuth2 authentication with Keycloak.
func SetupKeycloakAuth(ctx context.Context, k *KeycloakAuth) error {
	keycloak := k.config

	if keycloak.Token != "" {
		k.setToken(keycloak.Token)
		k.logger.Info("Keycloak authentication configured", "grantType", "direct_token")
		return nil
	}

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token",
		strings.TrimSuffix(keycloak.ServerURL, "/"),
		keycloak.Realm)

	if keycloak.Username != "" && keycloak.Password != "" {
		reqBody := fmt.Sprintf("grant_type=password&client_id=%s&client_secret=%s&username=%s&password=%s",
			keycloak.ClientID, keycloak.ClientSecret, keycloak.Username, keycloak.Password)

		req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(reqBody))
		if err != nil {
			return fmt.Errorf("failed to create token request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := k.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to get token from Keycloak: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("Keycloak token request failed with status %d: %s", resp.StatusCode, string(body))
		}

		var tokenResp struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int    `json:"expires_in"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
			return fmt.Errorf("failed to decode token response: %w", err)
		}

		k.setToken(tokenResp.AccessToken)
		go k.refreshTokenPasswordGrant(ctx, tokenURL, keycloak, tokenResp.RefreshToken, tokenResp.ExpiresIn)
		k.logger.Info("Keycloak authentication configured", "grantType", "password")
	} else if keycloak.ClientSecret != "" {
		config := &clientcredentials.Config{
			ClientID:     keycloak.ClientID,
			ClientSecret: keycloak.ClientSecret,
			TokenURL:     tokenURL,
		}

		tokenSource := config.TokenSource(ctx)
		k.tokenSource = tokenSource

		token, err := tokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to get token from Keycloak: %w", err)
		}

		k.setToken(token.AccessToken)
		go k.refreshToken(ctx, tokenSource)
		k.logger.Info("Keycloak authentication configured", "grantType", "client_credentials")
	} else {
		return fmt.Errorf("Keycloak authentication requires either token, username/password, or client secret")
	}

	return nil
}

// GetToken returns the current OAuth2 token (thread-safe).
func (k *KeycloakAuth) GetToken() string {
	k.tokenMu.RLock()
	defer k.tokenMu.RUnlock()
	return k.token
}

func (k *KeycloakAuth) setToken(token string) {
	k.tokenMu.Lock()
	defer k.tokenMu.Unlock()
	k.token = token
}

func (k *KeycloakAuth) refreshToken(ctx context.Context, tokenSource oauth2.TokenSource) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			token, err := tokenSource.Token()
			if err != nil {
				k.logger.Error(err, "Failed to refresh token")
				continue
			}
			k.setToken(token.AccessToken)
			k.logger.Info("Token refreshed successfully")
		}
	}
}

func (k *KeycloakAuth) refreshTokenPasswordGrant(ctx context.Context, tokenURL string, keycloak *v1.KeycloakConfig, refreshToken string, expiresIn int) {
	refreshInterval := time.Duration(expiresIn*80/100) * time.Second
	if refreshInterval < 1*time.Minute {
		refreshInterval = 1 * time.Minute
	}

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqBody := fmt.Sprintf("grant_type=refresh_token&client_id=%s&client_secret=%s&refresh_token=%s",
				keycloak.ClientID, keycloak.ClientSecret, refreshToken)

			req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(reqBody))
			if err != nil {
				k.logger.Error(err, "Failed to create refresh token request")
				continue
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			resp, err := k.httpClient.Do(req)
			if err != nil {
				k.logger.Error(err, "Failed to refresh token")
				continue
			}

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				k.logger.Error(nil, "Token refresh failed", "status", resp.StatusCode, "body", string(body))
				continue
			}

			var tokenResp struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				ExpiresIn    int    `json:"expires_in"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
				resp.Body.Close()
				k.logger.Error(err, "Failed to decode refresh token response")
				continue
			}
			resp.Body.Close()

			k.tokenMu.Lock()
			k.token = tokenResp.AccessToken
			if tokenResp.RefreshToken != "" {
				refreshToken = tokenResp.RefreshToken
			}
			if tokenResp.ExpiresIn > 0 {
				expiresIn = tokenResp.ExpiresIn
				refreshInterval = time.Duration(expiresIn*80/100) * time.Second
				if refreshInterval < 1*time.Minute {
					refreshInterval = 1 * time.Minute
				}
				ticker.Reset(refreshInterval)
			}
			k.tokenMu.Unlock()

			k.logger.Info("Token refreshed successfully")
		}
	}
}
