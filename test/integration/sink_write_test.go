//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/types"
)

func TestKafkaSinkWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	skipUnlessDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	kafkaContainer, err := kafka.RunContainer(ctx, kafka.WithClusterID("sink-write-cluster"))
	requireDocker(t, err)
	t.Cleanup(func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)
	require.NoError(t, waitForKafkaReady(ctx, brokers))

	sinkTopic := "sink-write-topic"
	require.NoError(t, createKafkaTopic(ctx, brokers, sinkTopic))

	sinkSpec := &v1.KafkaSinkSpec{
		Brokers: brokers,
		Topic:   sinkTopic,
	}
	sink := connectors.NewKafkaSinkConnector(sinkSpec)
	require.NoError(t, sink.Connect(ctx))
	t.Cleanup(func() { _ = sink.Close() })

	testMessages := []map[string]interface{}{
		{"id": 1, "name": "first"},
		{"id": 2, "name": "second"},
		{"id": 3, "name": "third"},
	}
	msgChan := make(chan *types.Message, len(testMessages))
	for _, payload := range testMessages {
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		msgChan <- types.NewMessage(body)
	}
	close(msgChan)

	require.NoError(t, sink.Write(ctx, msgChan))

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer(brokers, consumerConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	partitionConsumer, err := consumer.ConsumePartition(sinkTopic, 0, sarama.OffsetOldest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = partitionConsumer.Close() })

	received := make([]map[string]interface{}, 0, len(testMessages))
	deadline := time.After(15 * time.Second)
	for len(received) < len(testMessages) {
		select {
		case kafkaMsg := <-partitionConsumer.Messages():
			var row map[string]interface{}
			require.NoError(t, json.Unmarshal(kafkaMsg.Value, &row))
			received = append(received, row)
		case <-deadline:
			t.Fatalf("timeout waiting for %d messages, got %d", len(testMessages), len(received))
		}
	}

	ids := map[float64]bool{}
	for _, row := range received {
		id, ok := row["id"].(float64)
		require.True(t, ok)
		ids[id] = true
	}
	assert.True(t, ids[1])
	assert.True(t, ids[2])
	assert.True(t, ids[3])
}

func TestIcebergSinkWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	skipUnlessDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stack := startObjectStorageStack(ctx, t)
	catalogURI, _ := startIcebergRESTCatalog(ctx, t, stack)

	namespace := "integration_ns"
	table := "sink_events"
	autoCreate := true
	batchSize := int32(10)

	sinkSpec := &v1.IcebergSinkSpec{
		CatalogURI:    catalogURI,
		Namespace:     namespace,
		Table:         table,
		AutoCreateTable: &autoCreate,
		BatchSize:     &batchSize,
	}
	sink := connectors.NewIcebergSinkConnector(sinkSpec)
	require.NoError(t, sink.Connect(ctx))
	t.Cleanup(func() { _ = sink.Close() })

	payload := map[string]interface{}{"id": 42, "event": "iceberg-sink-write"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	msgChan := make(chan *types.Message, 1)
	msgChan <- types.NewMessage(body)
	close(msgChan)
	require.NoError(t, sink.Write(ctx, msgChan))

	pollInterval := int32(1)
	sourceSpec := &v1.IcebergSourceSpec{
		CatalogURI:   catalogURI,
		Namespace:    namespace,
		Table:        table,
		PollInterval: &pollInterval,
	}
	source := connectors.NewIcebergSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	t.Cleanup(func() { _ = source.Close() })

	msgOut, err := source.Read(ctx)
	require.NoError(t, err)

	var found bool
	readDeadline := time.After(45 * time.Second)
readLoop:
	for {
		select {
		case msg, ok := <-msgOut:
			if !ok {
				break readLoop
			}
			var row map[string]interface{}
			require.NoError(t, json.Unmarshal(msg.Data, &row))
			if id, _ := row["id"].(float64); id == 42 {
				assert.Equal(t, "iceberg-sink-write", row["event"])
				found = true
				break readLoop
			}
		case <-readDeadline:
			break readLoop
		}
	}
	assert.True(t, found, "expected row written by Iceberg sink to be readable via source")
}

func TestNessieSinkWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	skipUnlessDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stack := startObjectStorageStack(ctx, t)
	baseURL, _ := startNessieCatalog(ctx, t, stack)

	namespace := "integration_ns"
	table := "nessie_sink_events"
	autoCreate := true
	batchSize := int32(10)
	authNone := v1.NessieAuthenticationNone

	sinkSpec := &v1.NessieSinkSpec{
		BaseURL:         baseURL,
		Branch:          "main",
		Warehouse:       "warehouse",
		Namespace:       namespace,
		Table:           table,
		AutoCreateTable: &autoCreate,
		BatchSize:       &batchSize,
		AuthenticationType: authNone,
	}
	sink := connectors.NewNessieSinkConnector(sinkSpec)
	require.NoError(t, sink.Connect(ctx))
	t.Cleanup(func() { _ = sink.Close() })

	payload := map[string]interface{}{"id": 7, "event": "nessie-sink-write"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	msgChan := make(chan *types.Message, 1)
	msgChan <- types.NewMessage(body)
	close(msgChan)
	require.NoError(t, sink.Write(ctx, msgChan))

	pollInterval := int32(1)
	sourceSpec := &v1.NessieSourceSpec{
		BaseURL:      baseURL,
		Branch:        "main",
		Warehouse:     "warehouse",
		Namespace:     namespace,
		Table:         table,
		PollInterval: &pollInterval,
		AuthenticationType: authNone,
	}
	source := connectors.NewNessieSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	t.Cleanup(func() { _ = source.Close() })

	msgOut, err := source.Read(ctx)
	require.NoError(t, err)

	var found bool
	readDeadline := time.After(45 * time.Second)
readLoop:
	for {
		select {
		case msg, ok := <-msgOut:
			if !ok {
				break readLoop
			}
			var row map[string]interface{}
			require.NoError(t, json.Unmarshal(msg.Data, &row))
			if id, _ := row["id"].(float64); id == 7 {
				assert.Equal(t, "nessie-sink-write", row["event"])
				found = true
				break readLoop
			}
		case <-readDeadline:
			break readLoop
		}
	}
	assert.True(t, found, "expected row written by Nessie sink to be readable via source")
}

func waitForKafkaReady(ctx context.Context, brokers []string) error {
	maxRetries := 15
	retryDelay := 2 * time.Second
	for i := 0; i < maxRetries; i++ {
		adminConfig := sarama.NewConfig()
		adminConfig.Version = sarama.V2_8_0_0
		adminConfig.Net.DialTimeout = 5 * time.Second
		adminConfig.Net.ReadTimeout = 5 * time.Second
		admin, err := sarama.NewClusterAdmin(brokers, adminConfig)
		if err == nil {
			_, err = admin.DescribeTopics([]string{})
			admin.Close()
			if err == nil {
				return nil
			}
		}
		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}
	return context.DeadlineExceeded
}

func createKafkaTopic(ctx context.Context, brokers []string, topic string) error {
	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V2_8_0_0
	adminConfig.Net.DialTimeout = 10 * time.Second
	admin, err := sarama.NewClusterAdmin(brokers, adminConfig)
	if err != nil {
		return err
	}
	defer admin.Close()

	for i := 0; i < 5; i++ {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err == nil {
			return nil
		}
		errStr := err.Error()
		if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "TopicExistsException") {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}
