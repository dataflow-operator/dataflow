//go:build integration

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

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/IBM/sarama"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// TestKafkaConnectorIntegration tests Kafka source and sink connectors.
func TestKafkaConnectorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start Kafka container. Do not pass WithWaitStrategy: the kafka module uses a PostStarts
	// hook that copies a starter script and then waits for log "Transitioning from RECOVERY to RUNNING".
	// A custom ForListeningPort(9093) would block that hook (port only opens after the script runs).
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	// Wait for Kafka to be fully ready — verify availability via connection
	maxRetries := 15
	retryDelay := 2 * time.Second
	var kafkaReady bool
	for i := 0; i < maxRetries; i++ {
		adminConfig := sarama.NewConfig()
		adminConfig.Version = sarama.V2_8_0_0
		adminConfig.Net.DialTimeout = 5 * time.Second
		adminConfig.Net.ReadTimeout = 5 * time.Second
		admin, testErr := sarama.NewClusterAdmin(brokers, adminConfig)
		if testErr == nil {
			// Verify we can get metadata
			_, testErr = admin.DescribeTopics([]string{})
			if testErr == nil {
				admin.Close()
				kafkaReady = true
				break
			}
			admin.Close()
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	require.True(t, kafkaReady, "Kafka is not ready after %d retries", maxRetries)

	topic := "test-topic"
	consumerGroup := "test-group"

	// Create topic with retry logic
	adminConfig2 := sarama.NewConfig()
	adminConfig2.Version = sarama.V2_8_0_0
	adminConfig2.Net.DialTimeout = 10 * time.Second
	admin2, err := sarama.NewClusterAdmin(brokers, adminConfig2)
	require.NoError(t, err)
	defer admin2.Close()

	// Try to create topic several times
	for i := 0; i < 5; i++ {
		err = admin2.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err == nil {
			break
		}
		// Check if topic already exists — that's ok
		errStr := err.Error()
		if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "TopicExistsException") {
			err = nil
			break
		}
		if i < 4 {
			time.Sleep(2 * time.Second)
		}
	}
	// Ignore error if topic already exists
	if err != nil {
		t.Logf("Note: topic creation returned error (may already exist): %v", err)
	}

	t.Run("Kafka Source Connector", func(t *testing.T) {
		// Create source connector
		sourceSpec := &v1.KafkaSourceSpec{
			Brokers:       brokers,
			Topic:         topic,
			ConsumerGroup: consumerGroup,
		}
		sourceConnector := connectors.NewKafkaSourceConnector(sourceSpec)

		// Connect
		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)
		defer sourceConnector.Close()

		// Send test message to Kafka
		producerConfig := sarama.NewConfig()
		producerConfig.Producer.Return.Successes = true
		producerConfig.Version = sarama.V2_8_0_0
		producer, err := sarama.NewSyncProducer(brokers, producerConfig)
		require.NoError(t, err)
		defer producer.Close()

		testMessage := map[string]interface{}{
			"id":   1,
			"name": "test",
			"data": "test data",
		}
		messageBytes, err := json.Marshal(testMessage)
		require.NoError(t, err)

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		require.NoError(t, err)

		// Read message via source connector
		msgChan, err := sourceConnector.Read(ctx)
		require.NoError(t, err)

		// Wait for message with timeout
		select {
		case msg := <-msgChan:
			require.NotNil(t, msg)
			var receivedData map[string]interface{}
			err = json.Unmarshal(msg.Data, &receivedData)
			require.NoError(t, err)
			assert.Equal(t, float64(1), receivedData["id"])
			assert.Equal(t, "test", receivedData["name"])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("Kafka Sink Connector", func(t *testing.T) {
		sinkTopic := "sink-topic"
		err := admin2.CreateTopic(sinkTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		// Ignore error if topic already exists
		if err != nil {
			errStr := err.Error()
			if !strings.Contains(errStr, "already exists") && !strings.Contains(errStr, "TopicExistsException") {
				require.NoError(t, err)
			}
		}

		// Create sink connector
		sinkSpec := &v1.KafkaSinkSpec{
			Brokers: brokers,
			Topic:   sinkTopic,
		}
		sinkConnector := connectors.NewKafkaSinkConnector(sinkSpec)

		// Connect
		err = sinkConnector.Connect(ctx)
		require.NoError(t, err)
		defer sinkConnector.Close()

		// Create message for write
		testMessage := map[string]interface{}{
			"id":   2,
			"name": "sink test",
		}
		messageBytes, err := json.Marshal(testMessage)
		require.NoError(t, err)
		msg := types.NewMessage(messageBytes)

		// Write message
		msgChan := make(chan *types.Message, constants.DefaultSingleValueChannelBufferSize)
		msgChan <- msg
		close(msgChan)

		err = sinkConnector.Write(ctx, msgChan)
		require.NoError(t, err)

		// Verify message was written
		consumerConfig := sarama.NewConfig()
		consumerConfig.Version = sarama.V2_8_0_0
		consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		consumer, err := sarama.NewConsumer(brokers, consumerConfig)
		require.NoError(t, err)
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(sinkTopic, 0, sarama.OffsetOldest)
		require.NoError(t, err)
		defer partitionConsumer.Close()

		select {
		case kafkaMsg := <-partitionConsumer.Messages():
			var receivedData map[string]interface{}
			err = json.Unmarshal(kafkaMsg.Value, &receivedData)
			require.NoError(t, err)
			assert.Equal(t, float64(2), receivedData["id"])
			assert.Equal(t, "sink test", receivedData["name"])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})
}

// TestPostgreSQLConnectorIntegration tests PostgreSQL source and sink connectors.
func TestPostgreSQLConnectorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	tableName := "test_table"

	// Create table and insert test data
	// Try to connect several times with retry, as container may not be fully ready yet
	var conn *pgx.Conn
	maxRetries := 10
	retryDelay := 500 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		conn, err = pgx.Connect(ctx, connStr)
		if err == nil {
			// Verify connection via ping
			if pingErr := conn.Ping(ctx); pingErr == nil {
				break
			}
			conn.Close(ctx)
			conn = nil
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
			retryDelay *= 2 // exponential backoff
		}
	}
	require.NoError(t, err, "failed to connect to PostgreSQL after %d retries", maxRetries)
	require.NotNil(t, conn, "connection is nil")
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			value INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (name, value) VALUES
		('test1', 100),
		('test2', 200),
		('test3', 300)
	`, tableName))
	require.NoError(t, err)

	t.Run("PostgreSQL Source Connector", func(t *testing.T) {
		pollInterval := int32(1)
		sourceSpec := &v1.PostgreSQLSourceSpec{
			ConnectionString: connStr,
			Table:            tableName,
			PollInterval:     &pollInterval,
		}
		sourceConnector := connectors.NewPostgreSQLSourceConnector(sourceSpec)

		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)
		defer sourceConnector.Close()

		msgChan, err := sourceConnector.Read(ctx)
		require.NoError(t, err)

		// Read messages
		messages := make([]*types.Message, 0)
		timeout := time.After(5 * time.Second)
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					goto done
				}
				messages = append(messages, msg)
			case <-timeout:
				goto done
			}
		}
	done:
		require.GreaterOrEqual(t, len(messages), 3, "should read at least 3 messages")

		// Verify first message content
		var data map[string]interface{}
		err = json.Unmarshal(messages[0].Data, &data)
		require.NoError(t, err)
		assert.Contains(t, data, "name")
		assert.Contains(t, data, "value")
	})

	t.Run("PostgreSQL Sink Connector", func(t *testing.T) {
		sinkTable := "sink_table"
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INTEGER,
				name VARCHAR(100),
				value INTEGER
			)
		`, sinkTable))
		require.NoError(t, err)

		sinkSpec := &v1.PostgreSQLSinkSpec{
			ConnectionString: connStr,
			Table:            sinkTable,
		}
		sinkConnector := connectors.NewPostgreSQLSinkConnector(sinkSpec)

		err = sinkConnector.Connect(ctx)
		require.NoError(t, err)
		defer sinkConnector.Close()

		// Create messages for write
		testMessages := []map[string]interface{}{
			{"id": 1, "name": "sink1", "value": 10},
			{"id": 2, "name": "sink2", "value": 20},
		}

		msgChan := make(chan *types.Message, len(testMessages))
		for _, testMsg := range testMessages {
			msgBytes, err := json.Marshal(testMsg)
			require.NoError(t, err)
			msgChan <- types.NewMessage(msgBytes)
		}
		close(msgChan)

		err = sinkConnector.Write(ctx, msgChan)
		require.NoError(t, err)

		// Verify data was written
		var count int
		err = conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", sinkTable)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

// TestClickHouseConnectorIntegration tests ClickHouse source and sink connectors.
func TestClickHouseConnectorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// HTTP wait on 8123 with long timeout and poll interval for slow Docker envs.
	clickHouseContainer, err := clickhouse.Run(ctx, "clickhouse/clickhouse-server:23.3-alpine",
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword(""),
		clickhouse.WithDatabase("default"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort(nat.Port("8123/tcp")).
				WithStartupTimeout(5*time.Minute).
				WithPollInterval(2*time.Second),
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := clickHouseContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate clickhouse container: %v", err)
		}
	}()

	connStr, err := clickHouseContainer.ConnectionString(ctx)
	require.NoError(t, err)

	tableName := "test_table"

	// Create table and insert test data via sql.Open
	conn, err := sql.Open("clickhouse", connStr)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UInt64,
			name String,
			value Int32,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY id
	`, tableName))
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, name, value) VALUES
		(1, 'test1', 100),
		(2, 'test2', 200),
		(3, 'test3', 300)
	`, tableName))
	require.NoError(t, err)

	t.Run("ClickHouse Source Connector", func(t *testing.T) {
		pollInterval := int32(1)
		sourceSpec := &v1.ClickHouseSourceSpec{
			ConnectionString: connStr,
			Table:            tableName,
			PollInterval:     &pollInterval,
		}
		sourceConnector := connectors.NewClickHouseSourceConnector(sourceSpec)

		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)
		defer sourceConnector.Close()

		msgChan, err := sourceConnector.Read(ctx)
		require.NoError(t, err)

		messages := make([]*types.Message, 0)
		timeout := time.After(5 * time.Second)
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					goto done
				}
				messages = append(messages, msg)
			case <-timeout:
				goto done
			}
		}
	done:
		require.GreaterOrEqual(t, len(messages), 3, "should read at least 3 messages")

		var data map[string]interface{}
		err = json.Unmarshal(messages[0].Data, &data)
		require.NoError(t, err)
		assert.Contains(t, data, "name")
		assert.Contains(t, data, "value")
	})

	t.Run("ClickHouse Source Connector - Close does not block on read", func(t *testing.T) {
		// Verifies fix for mutex blocking: Close should complete quickly while readRows
		// executes a long query (conn and readState use separate mutexes).
		pollInterval := int32(1)
		sourceSpec := &v1.ClickHouseSourceSpec{
			ConnectionString: connStr,
			Table:            "system.numbers",
			Query:            "SELECT number FROM system.numbers LIMIT 100000000",
			PollInterval:     &pollInterval,
		}
		sourceConnector := connectors.NewClickHouseSourceConnector(sourceSpec)

		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)

		readCtx, cancelRead := context.WithCancel(ctx)
		msgChan, err := sourceConnector.Read(readCtx)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond) // let readRows start and run QueryContext

		closeDone := make(chan error, 1)
		closeStart := time.Now()
		go func() {
			closeDone <- sourceConnector.Close()
		}()

		select {
		case err := <-closeDone:
			elapsed := time.Since(closeStart)
			require.NoError(t, err)
			assert.Less(t, elapsed.Milliseconds(), int64(2000),
				"Close should not block on readRows; completed in %v", elapsed)
		case <-time.After(3 * time.Second):
			cancelRead()
			t.Fatal("Close blocked for more than 3 seconds - mutex blocking issue")
		}

		cancelRead()
		for range msgChan {
		}
	})

	t.Run("ClickHouse Sink Connector", func(t *testing.T) {
		sinkTable := "sink_table"
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				data String,
				created_at DateTime DEFAULT now()
			) ENGINE = MergeTree()
			ORDER BY created_at
		`, sinkTable))
		require.NoError(t, err)

		sinkSpec := &v1.ClickHouseSinkSpec{
			ConnectionString: connStr,
			Table:            sinkTable,
		}
		sinkConnector := connectors.NewClickHouseSinkConnector(sinkSpec)

		err = sinkConnector.Connect(ctx)
		require.NoError(t, err)
		defer sinkConnector.Close()

		testMessages := []map[string]interface{}{
			{"id": 1, "name": "sink1", "value": 10},
			{"id": 2, "name": "sink2", "value": 20},
		}

		msgChan := make(chan *types.Message, len(testMessages))
		for _, testMsg := range testMessages {
			msgBytes, err := json.Marshal(testMsg)
			require.NoError(t, err)
			msgChan <- types.NewMessage(msgBytes)
		}
		close(msgChan)

		err = sinkConnector.Write(ctx, msgChan)
		require.NoError(t, err)

		var count uint64
		err = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", sinkTable)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), count)
	})
}
