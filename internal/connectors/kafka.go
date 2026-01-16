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
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/hamba/avro/v2"
	"github.com/xdg-go/scram"
)

// KafkaSourceConnector implements SourceConnector for Kafka
type KafkaSourceConnector struct {
	config       *v1.KafkaSourceSpec
	consumer     sarama.ConsumerGroup
	closed       bool
	mu           sync.Mutex
	logger       logr.Logger
	avroSchema   avro.Schema           // Avro schema for deserialization (when not using Schema Registry)
	schemaCache  *schemaCache          // Cache for schemas from Schema Registry
	schemaClient *schemaRegistryClient // Client for Schema Registry
	namespace    string                // Namespace for metrics
	name         string                // Name for metrics
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
	return &KafkaSourceConnector{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the connector
func (k *KafkaSourceConnector) SetLogger(logger logr.Logger) {
	k.logger = logger
}

// SetMetadata sets the metadata for metrics
func (k *KafkaSourceConnector) SetMetadata(namespace, name string) {
	k.namespace = namespace
	k.name = name
}

// Connect establishes connection to Kafka
func (k *KafkaSourceConnector) Connect(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return fmt.Errorf("connector is closed")
	}

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

	// Configure TLS if provided
	if k.config.TLS != nil {
		// If CA certificate is provided, use it for verification (recommended by Yandex Cloud)
		// This overrides insecureSkipVerify to ensure proper certificate validation
		useInsecureSkipVerify := k.config.TLS.InsecureSkipVerify
		if k.config.TLS.CAFile != "" {
			useInsecureSkipVerify = false // Use CA certificate for verification
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: useInsecureSkipVerify,
			MinVersion:         tls.VersionTLS12, // Require TLS 1.2 or higher
		}

		if k.config.TLS.CAFile != "" {
			caCert, err := os.ReadFile(k.config.TLS.CAFile)
			if err != nil {
				k.logger.Error(err, "Failed to read CA file", "caFile", k.config.TLS.CAFile)
				return fmt.Errorf("failed to read CA file %s: %w", k.config.TLS.CAFile, err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				k.logger.Error(nil, "Failed to parse CA certificate", "caFile", k.config.TLS.CAFile)
				return fmt.Errorf("failed to parse CA certificate from file %s", k.config.TLS.CAFile)
			}
			tlsConfig.RootCAs = caCertPool
		} else if !k.config.TLS.InsecureSkipVerify {
			// If no CA file is provided and we're not skipping verification,
			// use system CA certificates
			caCertPool, err := x509.SystemCertPool()
			if err != nil {
				k.logger.Error(err, "Failed to load system CA certificates")
				return fmt.Errorf("failed to load system CA certificates: %w", err)
			}
			tlsConfig.RootCAs = caCertPool
		}

		if k.config.TLS.CertFile != "" && k.config.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(k.config.TLS.CertFile, k.config.TLS.KeyFile)
			if err != nil {
				k.logger.Error(err, "Failed to load certificate", "certFile", k.config.TLS.CertFile, "keyFile", k.config.TLS.KeyFile)
				return fmt.Errorf("failed to load certificate (cert: %s, key: %s): %w", k.config.TLS.CertFile, k.config.TLS.KeyFile, err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	// Configure SASL if provided
	if k.config.SASL != nil {
		// Validate SASL configuration
		if k.config.SASL.Username == "" {
			return fmt.Errorf("SASL username is required but not provided")
		}
		if k.config.SASL.Password == "" {
			k.logger.Error(nil, "SASL password is empty", "username", k.config.SASL.Username)
			return fmt.Errorf("SASL password is required but not provided (check if passwordSecretRef is correctly configured)")
		}

		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Handshake = true // Required for Yandex Cloud Kafka
		saramaConfig.Net.SASL.User = k.config.SASL.Username
		saramaConfig.Net.SASL.Password = k.config.SASL.Password

		// Set SASL mechanism based on configuration
		switch k.config.SASL.Mechanism {
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
			// Default to plaintext if not specified or explicitly set to plain
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s (supported: plain, scram-sha-256, scram-sha-512)", k.config.SASL.Mechanism)
		}
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
				// Record error metric
				if k.namespace != "" && k.name != "" {
					metrics.RecordConnectorError(k.namespace, k.name, "kafka", "source", "connect", "consumer_group_error")
				}
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

		// Record connection status
		if k.namespace != "" && k.name != "" {
			metrics.SetConnectorConnectionStatus(k.namespace, k.name, "kafka", "source", true)
		}

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

// Read returns a channel of messages from Kafka
func (k *KafkaSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if k.consumer == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *types.Message, 100)
	errorChan := make(chan error, 1)

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   msgChan,
		ready:     make(chan bool),
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := k.consumer.Consume(ctx, []string{k.config.Topic}, handler); err != nil {
					errorChan <- fmt.Errorf("error from consumer: %w", err)
					return
				}
			}
		}
	}()

	// Wait for consumer to be ready
	<-handler.ready

	// Handle errors
	go func() {
		for err := range k.consumer.Errors() {
			errorChan <- fmt.Errorf("consumer error: %w", err)
		}
	}()

	return msgChan, nil
}

// Close closes the Kafka connection
func (k *KafkaSourceConnector) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return nil
	}

	k.closed = true
	if k.consumer != nil {
		// Record connection status
		if k.namespace != "" && k.name != "" {
			metrics.SetConnectorConnectionStatus(k.namespace, k.name, "kafka", "source", false)
		}
		return k.consumer.Close()
	}
	return nil
}

// kafkaConsumerGroupHandler handles Kafka consumer group callbacks
type kafkaConsumerGroupHandler struct {
	connector *KafkaSourceConnector
	msgChan   chan *types.Message
	ready     chan bool
}

func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			var msgData []byte
			var err error

			// Deserialize based on format
			if h.connector.config.Format == "avro" {
				msgData, err = h.connector.deserializeAvro(session.Context(), message.Value)
				if err != nil {
					h.connector.logger.Error(err, "Failed to deserialize Avro message",
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

			select {
			case h.msgChan <- msg:
				session.MarkMessage(message, "")
				// Record metrics
				if h.connector.namespace != "" && h.connector.name != "" {
					metrics.RecordConnectorMessageRead(h.connector.namespace, h.connector.name, "kafka", "source")
				}
			case <-session.Context().Done():
				return nil
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

// KafkaSinkConnector implements SinkConnector for Kafka
type KafkaSinkConnector struct {
	config    *v1.KafkaSinkSpec
	producer  sarama.SyncProducer
	closed    bool
	mu        sync.Mutex
	logger    logr.Logger
	namespace string // Namespace for metrics
	name      string // Name for metrics
}

// NewKafkaSinkConnector creates a new Kafka sink connector
func NewKafkaSinkConnector(config *v1.KafkaSinkSpec) *KafkaSinkConnector {
	return &KafkaSinkConnector{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the connector
func (k *KafkaSinkConnector) SetLogger(logger logr.Logger) {
	k.logger = logger
}

// SetMetadata sets the metadata for metrics
func (k *KafkaSinkConnector) SetMetadata(namespace, name string) {
	k.namespace = namespace
	k.name = name
}

// Connect establishes connection to Kafka
func (k *KafkaSinkConnector) Connect(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return fmt.Errorf("connector is closed")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ClientID = "dataflow-operator" // Required for SASL authentication

	// Configure TLS if provided
	if k.config.TLS != nil {
		k.logger.Info("Configuring TLS", "insecureSkipVerify", k.config.TLS.InsecureSkipVerify, "caFile", k.config.TLS.CAFile)

		// If CA certificate is provided, use it for verification (recommended by Yandex Cloud)
		// This overrides insecureSkipVerify to ensure proper certificate validation
		useInsecureSkipVerify := k.config.TLS.InsecureSkipVerify
		if k.config.TLS.CAFile != "" {
			// When CA certificate is provided, prefer using it for verification
			// This matches Yandex Cloud documentation recommendations
			if k.config.TLS.InsecureSkipVerify {
				k.logger.Info("CA certificate provided but insecureSkipVerify is true. Using CA certificate for verification (recommended).")
			}
			useInsecureSkipVerify = false // Use CA certificate for verification
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: useInsecureSkipVerify,
			MinVersion:         tls.VersionTLS12, // Require TLS 1.2 or higher
		}

		if k.config.TLS.CAFile != "" {
			caCert, err := os.ReadFile(k.config.TLS.CAFile)
			if err != nil {
				k.logger.Error(err, "Failed to read CA file", "caFile", k.config.TLS.CAFile)
				return fmt.Errorf("failed to read CA file %s: %w", k.config.TLS.CAFile, err)
			}
			k.logger.Info("Read CA certificate", "caFile", k.config.TLS.CAFile, "size", len(caCert))
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				k.logger.Error(nil, "Failed to parse CA certificate", "caFile", k.config.TLS.CAFile)
				return fmt.Errorf("failed to parse CA certificate from file %s", k.config.TLS.CAFile)
			}
			tlsConfig.RootCAs = caCertPool
			k.logger.Info("Successfully loaded CA certificate", "usingForVerification", !useInsecureSkipVerify)
		} else if !k.config.TLS.InsecureSkipVerify {
			// If no CA file is provided and we're not skipping verification,
			// use system CA certificates
			k.logger.Info("Using system CA certificates")
			caCertPool, err := x509.SystemCertPool()
			if err != nil {
				k.logger.Error(err, "Failed to load system CA certificates")
				return fmt.Errorf("failed to load system CA certificates: %w", err)
			}
			tlsConfig.RootCAs = caCertPool
		}

		if k.config.TLS.CertFile != "" && k.config.TLS.KeyFile != "" {
			k.logger.Info("Loading client certificate", "certFile", k.config.TLS.CertFile, "keyFile", k.config.TLS.KeyFile)
			cert, err := tls.LoadX509KeyPair(k.config.TLS.CertFile, k.config.TLS.KeyFile)
			if err != nil {
				k.logger.Error(err, "Failed to load certificate", "certFile", k.config.TLS.CertFile, "keyFile", k.config.TLS.KeyFile)
				return fmt.Errorf("failed to load certificate (cert: %s, key: %s): %w", k.config.TLS.CertFile, k.config.TLS.KeyFile, err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
			k.logger.Info("Successfully loaded client certificate")
		}

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
		k.logger.Info("TLS enabled")
	} else {
		k.logger.Info("TLS not configured")
	}

	// Configure SASL if provided
	if k.config.SASL != nil {
		k.logger.Info("Configuring SASL", "mechanism", k.config.SASL.Mechanism, "username", k.config.SASL.Username)
		// Validate SASL configuration
		if k.config.SASL.Username == "" {
			return fmt.Errorf("SASL username is required but not provided")
		}
		if k.config.SASL.Password == "" {
			k.logger.Error(nil, "SASL password is empty", "username", k.config.SASL.Username)
			return fmt.Errorf("SASL password is required but not provided (check if passwordSecretRef is correctly configured)")
		}
		k.logger.Info("SASL password loaded", "passwordLength", len(k.config.SASL.Password))

		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Handshake = true // Required for Yandex Cloud Kafka
		saramaConfig.Net.SASL.User = k.config.SASL.Username
		saramaConfig.Net.SASL.Password = k.config.SASL.Password

		// Set SASL mechanism based on configuration
		switch k.config.SASL.Mechanism {
		case "scram-sha-256":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
			k.logger.Info("Using SCRAM-SHA-256")
		case "scram-sha-512":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
			k.logger.Info("Using SCRAM-SHA-512")
		case "plain", "":
			// Default to plaintext if not specified or explicitly set to plain
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			k.logger.Info("Using PLAIN")
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s (supported: plain, scram-sha-256, scram-sha-512)", k.config.SASL.Mechanism)
		}
	} else {
		k.logger.Info("SASL not configured")
	}

	// Validate brokers
	if len(k.config.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

		producer, err := sarama.NewSyncProducer(k.config.Brokers, saramaConfig)
		if err != nil {
			// Record error metric
			if k.namespace != "" && k.name != "" {
				metrics.RecordConnectorError(k.namespace, k.name, "kafka", "sink", "connect", "producer_error")
			}
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

		// Record connection status
		if k.namespace != "" && k.name != "" {
			metrics.SetConnectorConnectionStatus(k.namespace, k.name, "kafka", "sink", true)
		}

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

			partition, offset, err := k.producer.SendMessage(kafkaMsg)
			if err != nil {
				// Record error metric
				if k.namespace != "" && k.name != "" {
					metrics.RecordConnectorError(k.namespace, k.name, "kafka", "sink", "write", "send_error")
				}
				return fmt.Errorf("failed to send message: %w", err)
			}

			// Record metrics
			if k.namespace != "" && k.name != "" {
				route := getRouteFromMessage(msg)
				metrics.RecordConnectorMessageWritten(k.namespace, k.name, "kafka", "sink", route)
			}

			msg.Metadata["partition"] = partition
			msg.Metadata["offset"] = offset
		}
	}
}

// Close closes the Kafka connection
func (k *KafkaSinkConnector) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return nil
	}

	k.closed = true
	if k.producer != nil {
		// Record connection status
		if k.namespace != "" && k.name != "" {
			metrics.SetConnectorConnectionStatus(k.namespace, k.name, "kafka", "sink", false)
		}
		return k.producer.Close()
	}
	return nil
}

// getRouteFromMessage извлекает маршрут из метаданных сообщения
func getRouteFromMessage(msg *types.Message) string {
	if route, ok := msg.Metadata["routed_condition"].(string); ok {
		return route
	}
	return "default"
}
