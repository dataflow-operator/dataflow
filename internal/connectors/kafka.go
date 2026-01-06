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
	"fmt"
	"hash"
	"os"
	"sync"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/xdg-go/scram"
)

// KafkaSourceConnector implements SourceConnector for Kafka
type KafkaSourceConnector struct {
	config   *v1.KafkaSourceSpec
	consumer sarama.ConsumerGroup
	closed   bool
	mu       sync.Mutex
}

// NewKafkaSourceConnector creates a new Kafka source connector
func NewKafkaSourceConnector(config *v1.KafkaSourceSpec) *KafkaSourceConnector {
	return &KafkaSourceConnector{
		config: config,
	}
}

// Connect establishes connection to Kafka
func (k *KafkaSourceConnector) Connect(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return fmt.Errorf("connector is closed")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Configure TLS if provided
	if k.config.TLS != nil {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: k.config.TLS.InsecureSkipVerify,
		}

		if k.config.TLS.CAFile != "" {
			caCert, err := os.ReadFile(k.config.TLS.CAFile)
			if err != nil {
				return fmt.Errorf("failed to read CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if k.config.TLS.CertFile != "" && k.config.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(k.config.TLS.CertFile, k.config.TLS.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	// Configure SASL if provided
	if k.config.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = k.config.SASL.Username
		saramaConfig.Net.SASL.Password = k.config.SASL.Password
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext

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
		}
	}

	consumerGroup := k.config.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = "dataflow-operator"
	}

	consumer, err := sarama.NewConsumerGroup(k.config.Brokers, consumerGroup, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	k.consumer = consumer
	return nil
}

// Read returns a channel of messages from Kafka
func (k *KafkaSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if k.consumer == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *types.Message, 100)
	errorChan := make(chan error, 1)

	handler := &kafkaConsumerGroupHandler{
		msgChan: msgChan,
		ready:   make(chan bool),
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
		return k.consumer.Close()
	}
	return nil
}

// kafkaConsumerGroupHandler handles Kafka consumer group callbacks
type kafkaConsumerGroupHandler struct {
	msgChan chan *types.Message
	ready   chan bool
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
			msg := types.NewMessage(message.Value)
			msg.Metadata["topic"] = message.Topic
			msg.Metadata["partition"] = message.Partition
			msg.Metadata["offset"] = message.Offset
			msg.Metadata["key"] = string(message.Key)

			select {
			case h.msgChan <- msg:
				session.MarkMessage(message, "")
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
	config   *v1.KafkaSinkSpec
	producer sarama.SyncProducer
	closed   bool
	mu       sync.Mutex
}

// NewKafkaSinkConnector creates a new Kafka sink connector
func NewKafkaSinkConnector(config *v1.KafkaSinkSpec) *KafkaSinkConnector {
	return &KafkaSinkConnector{
		config: config,
	}
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

	// Configure TLS if provided
	if k.config.TLS != nil {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: k.config.TLS.InsecureSkipVerify,
		}

		if k.config.TLS.CAFile != "" {
			caCert, err := os.ReadFile(k.config.TLS.CAFile)
			if err != nil {
				return fmt.Errorf("failed to read CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if k.config.TLS.CertFile != "" && k.config.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(k.config.TLS.CertFile, k.config.TLS.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	// Configure SASL if provided
	if k.config.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = k.config.SASL.Username
		saramaConfig.Net.SASL.Password = k.config.SASL.Password
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext

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
		}
	}

	producer, err := sarama.NewSyncProducer(k.config.Brokers, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}

	k.producer = producer
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
				return fmt.Errorf("failed to send message: %w", err)
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
		return k.producer.Close()
	}
	return nil
}
