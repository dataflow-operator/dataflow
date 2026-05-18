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
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/hamba/avro/v2"
	"github.com/xdg-go/scram"
)

// KafkaSourceConnector implements SourceConnector for Kafka
type KafkaSourceConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config            *v1.KafkaSourceSpec
	consumer          sarama.ConsumerGroup
	channelBufferSize int
	avroSchema        avro.Schema           // Avro schema for deserialization (when not using Schema Registry)
	schemaCache       *schemaCache          // Cache for schemas from Schema Registry
	schemaClient      *schemaRegistryClient // Client for Schema Registry

	// consumeRetryDelay, if non-zero, is a fixed delay between Consume retries (coordinator/authorization),
	// instead of exponential backoff (kafkaConsumeRetryInitialBackoff / kafkaConsumeRetryMaxBackoff). Used in tests.
	consumeRetryDelay time.Duration
	// testConsumeFunc, if set, replaces consumer.Consume in Read (unit tests only).
	testConsumeFunc func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
}

// schemaCache caches Avro schemas by ID
type schemaCache struct {
	schemas map[int32]avro.Schema
	mu      sync.RWMutex
}

// schemaRegistryClient handles communication with Confluent Schema Registry
type schemaRegistryClient struct {
	url        string
	httpClient *http.Client
	authHeader string
	logger     logr.Logger
}

// NewKafkaSourceConnector creates a new Kafka source connector
func NewKafkaSourceConnector(config *v1.KafkaSourceSpec) *KafkaSourceConnector {
	return NewKafkaSourceConnectorWithOptions(config, nil)
}

// NewKafkaSourceConnectorWithOptions creates a new Kafka source connector with optional settings.
func NewKafkaSourceConnectorWithOptions(config *v1.KafkaSourceSpec, opts *SourceConnectorOptions) *KafkaSourceConnector {
	k := &KafkaSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "kafka", connectorRole: "source"},
	}
	if opts != nil && opts.ChannelBufferSize > 0 {
		k.channelBufferSize = opts.ChannelBufferSize
	} else {
		k.channelBufferSize = constants.DefaultChannelBufferSize
	}
	return k
}

// Connect establishes connection to Kafka
func (k *KafkaSourceConnector) Connect(ctx context.Context) error {
	if !k.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer k.Unlock()

	// Log connection attempt
	k.logger.Info("Connecting to Kafka",
		"brokers", k.config.Brokers,
		"topic", k.config.Topic)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Metadata.Full = true           // Required for Yandex Cloud Kafka
	saramaConfig.ClientID = "dataflow-operator" // Required for SASL authentication

	if err := applyKafkaTLS(k.config.TLS, saramaConfig, k.logger); err != nil {
		return err
	}
	if err := applyKafkaSASL(k.config.SASL, saramaConfig, k.logger); err != nil {
		return err
	}
	if err := applyKafkaConsumerConfig(k.config, saramaConfig); err != nil {
		return err
	}

	// Validate brokers
	if len(k.config.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

	consumerGroup := k.config.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = "dataflow-operator"
	}

	consumer, err := sarama.NewConsumerGroup(k.config.Brokers, consumerGroup, saramaConfig)
	if err != nil {
		k.RecordError("connect", "consumer_group_error")
		saslMechanism := "none"
		if k.config.SASL != nil {
			saslMechanism = k.config.SASL.Mechanism
			if saslMechanism == "" {
				saslMechanism = "plain"
			}
		}
		k.logger.Error(err, "Failed to create consumer group",
			"brokers", k.config.Brokers,
			"group", consumerGroup)
		return fmt.Errorf("failed to create consumer group (brokers: %v, group: %s, tls: %v, tlsSkipVerify: %v, sasl: %v, saslMechanism: %s, username: %s): %w",
			k.config.Brokers, consumerGroup, k.config.TLS != nil,
			k.config.TLS != nil && k.config.TLS.InsecureSkipVerify,
			k.config.SASL != nil, saslMechanism,
			func() string {
				if k.config.SASL != nil {
					return k.config.SASL.Username
				}
				return ""
			}(), err)
	}
	k.consumer = consumer
	k.logger.Info("Successfully connected to Kafka", "brokers", k.config.Brokers, "topic", k.config.Topic, "group", consumerGroup)
	k.SetConnectionStatus(true)

	// Initialize Schema Registry client if configured
	if k.config.Format == "avro" && k.config.SchemaRegistry != nil {
		if err := k.initSchemaRegistryClient(); err != nil {
			return fmt.Errorf("failed to initialize Schema Registry client: %w", err)
		}
		k.logger.Info("Schema Registry client initialized", "url", k.config.SchemaRegistry.URL)
	} else if k.config.Format == "avro" {
		// Load static Avro schema if Schema Registry is not configured
		if err := k.loadAvroSchema(); err != nil {
			return fmt.Errorf("failed to load Avro schema: %w", err)
		}
		k.logger.Info("Avro schema loaded successfully")
	}

	return nil
}

// initSchemaRegistryClient initializes the Schema Registry client
func (k *KafkaSourceConnector) initSchemaRegistryClient() error {
	if k.config.SchemaRegistry == nil {
		return fmt.Errorf("Schema Registry configuration is not provided")
	}

	url := k.config.SchemaRegistry.URL
	if url == "" {
		return fmt.Errorf("Schema Registry URL is required")
	}

	// Create HTTP client with TLS configuration
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Check if URL uses HTTPS
	usesHTTPS := len(url) >= 5 && url[:5] == "https"

	// Configure TLS if explicitly configured or if URL uses HTTPS
	if k.config.SchemaRegistry.TLS != nil || usesHTTPS {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if k.config.SchemaRegistry.TLS != nil {
			tlsConfig.InsecureSkipVerify = k.config.SchemaRegistry.TLS.InsecureSkipVerify

			if k.config.SchemaRegistry.TLS.CAFile != "" {
				caCert, err := os.ReadFile(k.config.SchemaRegistry.TLS.CAFile)
				if err != nil {
					return fmt.Errorf("failed to read CA file: %w", err)
				}
				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					return fmt.Errorf("failed to parse CA certificate")
				}
				tlsConfig.RootCAs = caCertPool
				// If CA file is provided, use it for verification (override insecureSkipVerify)
				if k.config.SchemaRegistry.TLS.CAFile != "" {
					tlsConfig.InsecureSkipVerify = false
				}
			} else if !k.config.SchemaRegistry.TLS.InsecureSkipVerify {
				// Use system CA certificates if not skipping verification and no CA file provided
				caCertPool, err := x509.SystemCertPool()
				if err != nil {
					k.logger.Info("Failed to load system CA certificates, using default", "error", err)
				} else {
					tlsConfig.RootCAs = caCertPool
				}
			}
		} else if usesHTTPS {
			// HTTPS URL but no TLS config - for convenience, use insecure connection by default
			// This allows connection to services with self-signed or internal CA certificates (e.g., Yandex Cloud)
			// For production, user should explicitly configure TLS with insecureSkipVerify: true or provide CA file
			k.logger.Info("HTTPS URL detected but TLS not configured. Using insecure TLS connection. For production, configure TLS explicitly")
			tlsConfig.InsecureSkipVerify = true
		}

		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	// Setup basic auth if configured
	var authHeader string
	if k.config.SchemaRegistry.BasicAuth != nil {
		username := k.config.SchemaRegistry.BasicAuth.Username
		password := k.config.SchemaRegistry.BasicAuth.Password
		if username != "" && password != "" {
			auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
			authHeader = fmt.Sprintf("Basic %s", auth)
			k.logger.Info("Schema Registry Basic Auth configured", "username", username)
		} else {
			k.logger.Info("Schema Registry Basic Auth section present but username/password not provided, connecting without authentication")
		}
	} else {
		k.logger.Info("Schema Registry Basic Auth not configured, connecting without authentication")
	}

	k.schemaClient = &schemaRegistryClient{
		url:        url,
		httpClient: httpClient,
		authHeader: authHeader,
		logger:     k.logger,
	}

	k.schemaCache = &schemaCache{
		schemas: make(map[int32]avro.Schema),
	}

	return nil
}

// getSchemaFromRegistry fetches schema from Schema Registry by ID
func (c *schemaRegistryClient) getSchemaByID(ctx context.Context, schemaID int32) (avro.Schema, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.url, schemaID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Check if error is related to TLS certificate verification
		errMsg := err.Error()
		if strings.Contains(errMsg, "certificate") || strings.Contains(errMsg, "x509") {
			return nil, fmt.Errorf("TLS certificate verification failed: %w. Configure TLS with insecureSkipVerify: true or provide CA file in schemaRegistry.tls", err)
		}
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Schema Registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var schemaResponse struct {
		Schema string `json:"schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&schemaResponse); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	// Parse Avro schema
	schema, err := avro.Parse(schemaResponse.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	return schema, nil
}

// normalizeAvroArrays recursively normalizes Avro arrays wrapped in objects with "array" field
// hamba/avro wraps arrays as {"array": [...]}, we convert them back to [...]
func (k *KafkaSourceConnector) normalizeAvroArrays(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if this is a wrapped array: object with single "array" key that contains an array
		if len(v) == 1 {
			if arrayVal, ok := v["array"]; ok {
				if arr, ok := arrayVal.([]interface{}); ok {
					// This is a wrapped array, return the array directly
					// Recursively normalize elements in the array
					normalized := make([]interface{}, len(arr))
					for i, item := range arr {
						normalized[i] = k.normalizeAvroArrays(item)
					}
					return normalized
				}
			}
		}
		// Normal map, normalize all values recursively
		normalized := make(map[string]interface{})
		for key, val := range v {
			normalized[key] = k.normalizeAvroArrays(val)
		}
		return normalized
	case []interface{}:
		// Array, normalize all elements recursively
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			normalized[i] = k.normalizeAvroArrays(item)
		}
		return normalized
	default:
		// Primitive value, return as-is
		return v
	}
}

// getCachedSchema gets schema from cache or fetches from Registry
func (k *KafkaSourceConnector) getCachedSchema(ctx context.Context, schemaID int32) (avro.Schema, error) {
	// Check cache first
	k.schemaCache.mu.RLock()
	if schema, ok := k.schemaCache.schemas[schemaID]; ok {
		k.schemaCache.mu.RUnlock()
		return schema, nil
	}
	k.schemaCache.mu.RUnlock()

	// Fetch from Registry
	schema, err := k.schemaClient.getSchemaByID(ctx, schemaID)
	if err != nil {
		return nil, err
	}

	// Cache the schema
	k.schemaCache.mu.Lock()
	k.schemaCache.schemas[schemaID] = schema
	k.schemaCache.mu.Unlock()

	return schema, nil
}

// loadAvroSchema loads the Avro schema from configuration
func (k *KafkaSourceConnector) loadAvroSchema() error {
	var schemaStr string

	// Try to get schema from different sources
	if k.config.AvroSchema != "" {
		schemaStr = k.config.AvroSchema
	} else if k.config.AvroSchemaFile != "" {
		schemaBytes, err := os.ReadFile(k.config.AvroSchemaFile)
		if err != nil {
			return fmt.Errorf("failed to read Avro schema file %s: %w", k.config.AvroSchemaFile, err)
		}
		schemaStr = string(schemaBytes)
	} else if k.config.AvroSchemaSecretRef != nil {
		// TODO: Implement secret reading from Kubernetes
		return fmt.Errorf("AvroSchemaSecretRef is not yet implemented")
	} else {
		return fmt.Errorf("Avro schema is required when format is 'avro'. Provide avroSchema, avroSchemaFile, or avroSchemaSecretRef")
	}

	// Parse Avro schema
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		return fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	k.avroSchema = schema
	return nil
}

// deserializeAvro deserializes Avro message to JSON
// Supports both raw Avro and Confluent Schema Registry format (magic byte + schema ID + data)
func (k *KafkaSourceConnector) deserializeAvro(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty Avro data")
	}

	var schema avro.Schema
	var avroData []byte

	// Check if this is Confluent Schema Registry format (magic byte 0x00)
	// Format: [magic byte (1 byte)] [schema ID (4 bytes, big-endian)] [avro data]
	if data[0] == 0x00 && len(data) > 5 {
		// Extract schema ID (4 bytes, big-endian)
		schemaID := int32(binary.BigEndian.Uint32(data[1:5]))
		avroData = data[5:]

		k.logger.V(1).Info("Detected Confluent Schema Registry format", "schemaID", schemaID)

		// Get schema from Registry (with caching)
		var err error
		if k.schemaClient != nil {
			schema, err = k.getCachedSchema(ctx, schemaID)
			if err != nil {
				return nil, fmt.Errorf("failed to get schema from Registry (ID: %d): %w", schemaID, err)
			}
		} else {
			return nil, fmt.Errorf("Schema Registry is not configured but message uses Schema Registry format (schema ID: %d)", schemaID)
		}
	} else {
		// Raw Avro format - use static schema
		if k.avroSchema == nil {
			return nil, fmt.Errorf("Avro schema is not loaded and Schema Registry is not configured")
		}
		schema = k.avroSchema
		avroData = data
	}

	// Deserialize Avro data using hamba/avro
	var result map[string]interface{}
	if err := avro.Unmarshal(schema, avroData, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Avro data: %w", err)
	}

	// Normalize Avro arrays: hamba/avro wraps arrays in objects with "array" field
	// Convert {"array": [...]} back to [...] for all fields
	normalized := k.normalizeAvroArrays(result)
	if normalizedMap, ok := normalized.(map[string]interface{}); ok {
		result = normalizedMap
	} else {
		// This shouldn't happen for top-level objects, but handle it gracefully
		k.logger.V(1).Info("Normalized result is not a map, using original result")
	}

	// Convert to JSON
	jsonData, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Avro result to JSON: %w", err)
	}

	return jsonData, nil
}

// Backoff for Kafka consumer group Consume(): exponential delay capped at kafkaConsumeRetryMaxBackoff,
// repeating until the parent context is cancelled (no fixed attempt limit).
const (
	kafkaConsumeRetryInitialBackoff = 1 * time.Second
	kafkaConsumeRetryMaxBackoff     = 2 * time.Minute
)

// Read returns a channel of messages from Kafka
func (k *KafkaSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if k.consumer == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *types.Message, k.channelBufferSize)
	errCh := make(chan error, constants.DefaultSingleValueChannelBufferSize)

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   msgChan,
		ready:     make(chan bool),
	}

	consumeFn := func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		return k.consumer.Consume(cctx, topics, h)
	}
	if k.testConsumeFunc != nil {
		consumeFn = k.testConsumeFunc
	}

	go func() {
		backoff := kafkaConsumeRetryInitialBackoff
		if k.consumeRetryDelay > 0 {
			backoff = k.consumeRetryDelay
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			consumeErr := consumeFn(ctx, []string{k.config.Topic}, handler)
			if consumeErr == nil {
				return
			}
			if errors.Is(consumeErr, context.Canceled) {
				return
			}
			if errors.Is(consumeErr, context.DeadlineExceeded) {
				errWrap := fmt.Errorf("error from consumer: %w", consumeErr)
				k.logger.Error(errWrap, "Kafka consumer Consume failed", "topic", k.config.Topic)
				errCh <- errWrap
				return
			}
			if isKafkaConsumeRetriableError(consumeErr) {
				var delay time.Duration
				if k.consumeRetryDelay > 0 {
					delay = k.consumeRetryDelay
				} else {
					delay = backoff
					if delay > kafkaConsumeRetryMaxBackoff {
						delay = kafkaConsumeRetryMaxBackoff
					}
				}
				reason := "transient"
				if isKafkaAuthorizationError(consumeErr) {
					reason = "authorization"
				} else if isCoordinatorUnavailableError(consumeErr) {
					reason = "coordinator_unavailable"
				}
				k.logger.Info("Kafka consumer Consume failed, retrying with backoff",
					"topic", k.config.Topic, "reason", reason, "backoff", delay, "err", consumeErr)
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
				if k.consumeRetryDelay <= 0 {
					next := backoff * 2
					if next < backoff { // overflow
						backoff = kafkaConsumeRetryMaxBackoff
					} else if next > kafkaConsumeRetryMaxBackoff {
						backoff = kafkaConsumeRetryMaxBackoff
					} else {
						backoff = next
					}
				}
				continue
			}
			errWrap := fmt.Errorf("error from consumer: %w", consumeErr)
			k.logger.Error(errWrap, "Kafka consumer Consume failed", "topic", k.config.Topic)
			errCh <- errWrap
			return
		}
	}()

	// Wait until Setup closes ready, Consume reports a fatal error, or context ends (avoid deadlock if Consume fails before Setup).
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case <-handler.ready:
	}

	// Handle errors
	go func() {
		for err := range k.consumer.Errors() {
			if isKafkaRequestTimedOutError(err) {
				k.RecordError("read", "request_timed_out")
				k.logger.Info("Kafka consumer fetch timed out (transient); check broker load, consumer lag, and consider increasing source.consumerMaxWait",
					"topic", k.config.Topic, "err", err)
				continue
			}
			errWrap := fmt.Errorf("consumer error: %w", err)
			k.logger.Error(errWrap, "Kafka consumer error", "topic", k.config.Topic)
			select {
			case errCh <- errWrap:
			default:
				k.logger.Error(errWrap, "Kafka consumer error dropped (error channel full)", "topic", k.config.Topic)
			}
		}
	}()

	return msgChan, nil
}

// applyKafkaConsumerConfig maps optional KafkaSourceSpec consumer tuning to Sarama.
func applyKafkaConsumerConfig(spec *v1.KafkaSourceSpec, cfg *sarama.Config) error {
	if spec == nil {
		return nil
	}
	if spec.ConsumerMaxWait != nil {
		d := spec.ConsumerMaxWait.Duration
		if d <= 0 {
			return fmt.Errorf("consumerMaxWait must be positive")
		}
		cfg.Consumer.MaxWaitTime = d
	}
	if spec.FetchMinBytes != nil {
		if *spec.FetchMinBytes < 0 {
			return fmt.Errorf("fetchMinBytes must be >= 0")
		}
		cfg.Consumer.Fetch.Min = *spec.FetchMinBytes
	}
	if spec.FetchMaxBytes != nil {
		if *spec.FetchMaxBytes <= 0 {
			return fmt.Errorf("fetchMaxBytes must be positive")
		}
		cfg.Consumer.Fetch.Default = *spec.FetchMaxBytes
	}
	if spec.MaxPartitionFetchBytes != nil {
		if *spec.MaxPartitionFetchBytes <= 0 {
			return fmt.Errorf("maxPartitionFetchBytes must be positive")
		}
		cfg.Consumer.Fetch.Max = *spec.MaxPartitionFetchBytes
	}
	if spec.NetReadTimeout != nil {
		d := spec.NetReadTimeout.Duration
		if d <= 0 {
			return fmt.Errorf("netReadTimeout must be positive")
		}
		cfg.Net.ReadTimeout = d
	}
	if spec.NetWriteTimeout != nil {
		d := spec.NetWriteTimeout.Duration
		if d <= 0 {
			return fmt.Errorf("netWriteTimeout must be positive")
		}
		cfg.Net.WriteTimeout = d
	}
	if spec.ConsumerMaxWait != nil && spec.NetReadTimeout != nil && cfg.Net.ReadTimeout <= cfg.Consumer.MaxWaitTime {
		return fmt.Errorf("netReadTimeout must be greater than consumerMaxWait")
	}
	return nil
}

// isKafkaRequestTimedOutError reports Kafka REQUEST_TIMED_OUT (broker could not complete Fetch in max.wait).
func isKafkaRequestTimedOutError(err error) bool {
	if err == nil {
		return false
	}
	var ke sarama.KError
	if errors.As(err, &ke) && ke == sarama.ErrRequestTimedOut {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "request exceeded the user-specified time limit") ||
		strings.Contains(lower, "request_timed_out")
}

// isCoordinatorUnavailableError reports whether err is Kafka "coordinator is not available" (retriable).
func isCoordinatorUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "coordinator is not available") ||
		strings.Contains(s, "CoordinatorNotAvailable")
}

// isKafkaAuthorizationError reports topic/group (and related) authorization failures that may clear
// when ACLs are fixed without redeploying the operator.
func isKafkaAuthorizationError(err error) bool {
	if err == nil {
		return false
	}
	var ke sarama.KError
	if errors.As(err, &ke) {
		switch ke {
		case sarama.ErrTopicAuthorizationFailed,
			sarama.ErrGroupAuthorizationFailed,
			sarama.ErrClusterAuthorizationFailed,
			sarama.ErrTransactionalIDAuthorizationFailed,
			sarama.ErrDelegationTokenAuthorizationFailed:
			return true
		}
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "topic_authorization_failed") ||
		strings.Contains(lower, "group_authorization_failed") ||
		strings.Contains(lower, "cluster_authorization_failed") ||
		strings.Contains(lower, "not authorized") ||
		strings.Contains(lower, "authorization failed")
}

// isKafkaConsumeRetriableError reports coordinator-unavailable and topic/group authorization failures.
// Read retries those with exponential backoff capped at kafkaConsumeRetryMaxBackoff until ctx is cancelled.
func isKafkaConsumeRetriableError(err error) bool {
	return isCoordinatorUnavailableError(err) || isKafkaAuthorizationError(err)
}

// Close closes the Kafka connection
func (k *KafkaSourceConnector) Close() error {
	if k.guardClose() {
		return nil
	}
	defer k.Unlock()

	k.logger.Info("Closing Kafka source connection", "brokers", k.config.Brokers, "topic", k.config.Topic)
	if k.consumer != nil {
		k.SetConnectionStatus(false)
		return k.consumer.Close()
	}
	return nil
}

// kafkaConsumerGroupHandler handles Kafka consumer group callbacks
type kafkaConsumerGroupHandler struct {
	connector *KafkaSourceConnector
	msgChan   chan *types.Message
	ready     chan bool
	readyOnce sync.Once // Protects ready channel from being closed multiple times
}

func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Use sync.Once to ensure channel is closed only once
	// This protects against multiple Setup calls during rebalancing
	// sync.Once guarantees the function passed to Do will execute exactly once,
	// even if Setup is called concurrently from multiple goroutines
	h.readyOnce.Do(func() {
		// Use recover to handle potential panic if channel is already closed
		// This can happen in rare race conditions during rebalancing
		defer func() {
			if r := recover(); r != nil {
				// Channel was already closed, which is fine - just log and continue
				// This should not happen with sync.Once, but we handle it gracefully
				if h.connector != nil {
					h.connector.logger.V(1).Info("Channel already closed in Setup (recovered from panic)", "error", r)
				}
			}
		}()
		close(h.ready)
	})
	return nil
}

func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// kafkaMarkChannelBuffer is the buffer for deferred offset marks from sink Ack callbacks.
// Marks are processed in ConsumeClaim (same goroutine as Sarama expects) to avoid blocking MarkMessage from other goroutines.
const kafkaMarkChannelBuffer = 256

func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	markChan := make(chan *sarama.ConsumerMessage, kafkaMarkChannelBuffer)

	markPending := func(message *sarama.ConsumerMessage) {
		select {
		case markChan <- message:
		case <-session.Context().Done():
		}
	}

	drainMarks := func() {
		for {
			select {
			case m := <-markChan:
				session.MarkMessage(m, "")
			default:
				return
			}
		}
	}

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				drainMarks()
				return nil
			}

			var msgData []byte
			var err error

			// Deserialize based on format
			if h.connector.config.Format == "avro" {
				msgData, err = h.connector.deserializeAvro(session.Context(), message.Value)
				if err != nil {
					h.connector.logger.Error(err, "Failed to deserialize Avro message",
						logkeys.MessageID, fmt.Sprintf("%d/%d", message.Partition, message.Offset),
						"topic", message.Topic,
						"partition", message.Partition,
						"offset", message.Offset)
					// Skip this message but continue processing
					session.MarkMessage(message, "")
					continue
				}
			} else {
				// Default: use message value as-is (JSON or other format)
				msgData = message.Value
			}

			msg := types.NewMessage(msgData)
			msg.Metadata["topic"] = message.Topic
			msg.Metadata["partition"] = message.Partition
			msg.Metadata["offset"] = message.Offset
			msg.Metadata["key"] = string(message.Key)
			msg.Metadata["timestamp"] = message.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z07:00")
			// Commit offset only after the message is successfully written to the sink.
			// Mark is queued and applied in this goroutine (not from sink goroutine).
			msg.Ack = func() { markPending(message) }

			enqueued := false
			for !enqueued {
				select {
				case h.msgChan <- msg:
					h.connector.RecordMessageRead()
					enqueued = true
				case m := <-markChan:
					session.MarkMessage(m, "")
				case <-session.Context().Done():
					return nil
				}
			}
		case m := <-markChan:
			session.MarkMessage(m, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// KafkaSinkConnector implements SinkConnector for Kafka
type KafkaSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config   *v1.KafkaSinkSpec
	producer sarama.SyncProducer
}

// NewKafkaSinkConnector creates a new Kafka sink connector
func NewKafkaSinkConnector(config *v1.KafkaSinkSpec) *KafkaSinkConnector {
	return &KafkaSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "kafka", connectorRole: "sink"},
	}
}

// Connect establishes connection to Kafka
func (k *KafkaSinkConnector) Connect(ctx context.Context) error {
	if !k.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer k.Unlock()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Idempotent = true     // Prevents duplicate messages on retry
	saramaConfig.Net.MaxOpenRequests = 1        // Required for idempotent producer ordering
	saramaConfig.ClientID = "dataflow-operator" // Required for SASL authentication

	if err := applyKafkaTLS(k.config.TLS, saramaConfig, k.logger); err != nil {
		return err
	}
	if err := applyKafkaSASL(k.config.SASL, saramaConfig, k.logger); err != nil {
		return err
	}

	// Validate brokers
	if len(k.config.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

	producer, err := sarama.NewSyncProducer(k.config.Brokers, saramaConfig)
	if err != nil {
		k.RecordError("connect", "producer_error")
		saslMechanism := "none"
		if k.config.SASL != nil {
			saslMechanism = k.config.SASL.Mechanism
			if saslMechanism == "" {
				saslMechanism = "plain"
			}
		}
		k.logger.Error(err, "Failed to create producer",
			"brokers", k.config.Brokers)
		return fmt.Errorf("failed to create producer (brokers: %v, tls: %v, tlsSkipVerify: %v, sasl: %v, saslMechanism: %s, username: %s): %w",
			k.config.Brokers, k.config.TLS != nil,
			k.config.TLS != nil && k.config.TLS.InsecureSkipVerify,
			k.config.SASL != nil, saslMechanism,
			func() string {
				if k.config.SASL != nil {
					return k.config.SASL.Username
				}
				return ""
			}(), err)
	}
	k.producer = producer
	k.logger.Info("Successfully connected to Kafka", "brokers", k.config.Brokers, "topic", k.config.Topic)
	k.SetConnectionStatus(true)

	return nil
}

// XDGSCRAMClient implements sarama.SCRAMClient for SCRAM authentication
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin prepares the client for the SCRAM exchange
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step continues the SCRAM exchange
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done returns true if the SCRAM exchange is complete
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// SHA256 hash generator function for SCRAM-SHA-256
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 hash generator function for SCRAM-SHA-512
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

// Write writes messages to Kafka
func (k *KafkaSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if k.producer == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			kafkaMsg := &sarama.ProducerMessage{
				Topic: k.config.Topic,
				Value: sarama.ByteEncoder(msg.Data),
			}

			// Add key from metadata if present
			if key, ok := msg.Metadata["key"].(string); ok {
				kafkaMsg.Key = sarama.StringEncoder(key)
			}

			var partition int32
			var offset int64
			err := retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
				p, o, sendErr := k.producer.SendMessage(kafkaMsg)
				if sendErr != nil {
					return sendErr
				}
				partition, offset = p, o
				return nil
			})
			if err != nil {
				k.RecordError("write", "send_error")
				return fmt.Errorf("failed to send message: %w", err)
			}

			k.RecordMessageWritten(getRouteFromMessage(msg))

			msg.Metadata["partition"] = partition
			msg.Metadata["offset"] = offset

			if msg.Ack != nil {
				msg.Ack()
			}
		}
	}
}

// Close closes the Kafka connection
func (k *KafkaSinkConnector) Close() error {
	if k.guardClose() {
		return nil
	}
	defer k.Unlock()

	k.logger.Info("Closing Kafka sink connection", "brokers", k.config.Brokers, "topic", k.config.Topic)
	if k.producer != nil {
		k.SetConnectionStatus(false)
		return k.producer.Close()
	}
	return nil
}

// getRouteFromMessage extracts the route from message metadata.
func getRouteFromMessage(msg *types.Message) string {
	if route, ok := msg.Metadata["routed_condition"].(string); ok {
		return route
	}
	return "default"
}

// applyKafkaTLS configures TLS on sarama config from TLSConfig.
func applyKafkaTLS(tlsConfig *v1.TLSConfig, saramaConfig *sarama.Config, logger logr.Logger) error {
	if tlsConfig == nil {
		return nil
	}
	useInsecureSkipVerify := tlsConfig.InsecureSkipVerify
	if tlsConfig.CAFile != "" {
		useInsecureSkipVerify = false
	}
	config := &tls.Config{
		InsecureSkipVerify: useInsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
	if tlsConfig.CAFile != "" {
		caCert, err := os.ReadFile(tlsConfig.CAFile)
		if err != nil {
			logger.Error(err, "Failed to read CA file", "caFile", tlsConfig.CAFile)
			return fmt.Errorf("failed to read CA file %s: %w", tlsConfig.CAFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			logger.Error(nil, "Failed to parse CA certificate", "caFile", tlsConfig.CAFile)
			return fmt.Errorf("failed to parse CA certificate from file %s", tlsConfig.CAFile)
		}
		config.RootCAs = caCertPool
	} else if !tlsConfig.InsecureSkipVerify {
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			logger.Error(err, "Failed to load system CA certificates")
			return fmt.Errorf("failed to load system CA certificates: %w", err)
		}
		config.RootCAs = caCertPool
	}
	if tlsConfig.CertFile != "" && tlsConfig.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
		if err != nil {
			logger.Error(err, "Failed to load certificate", "certFile", tlsConfig.CertFile, "keyFile", tlsConfig.KeyFile)
			return fmt.Errorf("failed to load certificate (cert: %s, key: %s): %w", tlsConfig.CertFile, tlsConfig.KeyFile, err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = config
	return nil
}

// applyKafkaSASL configures SASL on sarama config from SASLConfig.
func applyKafkaSASL(saslConfig *v1.SASLConfig, saramaConfig *sarama.Config, logger logr.Logger) error {
	if saslConfig == nil {
		return nil
	}
	if saslConfig.Username == "" {
		return fmt.Errorf("SASL username is required but not provided")
	}
	if saslConfig.Password == "" {
		logger.Error(nil, "SASL password is empty", "username", saslConfig.Username)
		return fmt.Errorf("SASL password is required but not provided (check if passwordSecretRef is correctly configured)")
	}
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.Handshake = true
	saramaConfig.Net.SASL.User = saslConfig.Username
	saramaConfig.Net.SASL.Password = saslConfig.Password
	switch saslConfig.Mechanism {
	case "scram-sha-256":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
	case "scram-sha-512":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	case "plain", "":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s (supported: plain, scram-sha-256, scram-sha-512)", saslConfig.Mechanism)
	}
	return nil
}
