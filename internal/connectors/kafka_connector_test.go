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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testConsumerGroup is a minimal sarama.ConsumerGroup for Read tests (Consume is overridden via testConsumeFunc).
type testConsumerGroup struct {
	errorsCh <-chan error
}

func (testConsumerGroup) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	panic("testConsumerGroup.Consume must be replaced by KafkaSourceConnector.testConsumeFunc in tests")
}

func (t testConsumerGroup) Errors() <-chan error    { return t.errorsCh }
func (testConsumerGroup) Pause(map[string][]int32)  {}
func (testConsumerGroup) Resume(map[string][]int32) {}
func (testConsumerGroup) PauseAll()                 {}
func (testConsumerGroup) ResumeAll()                {}
func (testConsumerGroup) Close() error              { return nil }

var _ SourceReadErrors = (*KafkaSourceConnector)(nil)

func TestKafkaRead_AsyncFatalErrorOnReadErrors(t *testing.T) {
	asyncErrCh := make(chan error, 1)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: asyncErrCh}

	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh, err := k.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if msgCh == nil {
		t.Fatal("expected non-nil message channel")
	}
	readErrors := k.ReadErrors()
	if readErrors == nil {
		t.Fatal("expected ReadErrors channel after Read")
	}

	asyncErrCh <- errors.New("unknown topic or partition")

	select {
	case got := <-readErrors:
		if got == nil {
			t.Fatal("expected non-nil error on ReadErrors")
		}
		if !strings.Contains(got.Error(), "unknown topic") {
			t.Fatalf("ReadErrors = %v, want unknown topic error", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fatal async consumer error on ReadErrors")
	}

	cancel()
}

func TestKafkaRead_ConsumeFatalErrorBeforeSetup_ReturnsError(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.testConsumeFunc = func(context.Context, []string, sarama.ConsumerGroupHandler) error {
		return errors.New("unknown topic or partition")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	readDone := make(chan error, 1)
	go func() {
		_, err := k.Read(ctx)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected Read error for fatal Consume failure before Setup")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Read blocked; expected quick return on fatal Consume error before Setup")
	}
}

func TestKafkaRead_AuthorizationErrorRetriesThenReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("broker: %w", sarama.ErrTopicAuthorizationFailed)
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	var msgCh <-chan *types.Message
	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		msgCh = res.ch
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after authorization retry and Setup")
	}

	if msgCh == nil {
		t.Fatal("expected non-nil message channel")
	}
	if calls != 2 {
		t.Fatalf("Consume calls = %d, want 2 (one auth failure, one success)", calls)
	}

	cancel()
}

func TestApplyKafkaNetworkConfig_Implicit(t *testing.T) {
	cfg := sarama.NewConfig()
	sasl := &v1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "user",
		Password:  "pass",
	}
	if err := applyKafkaNetworkConfig(nil, sasl, "", cfg, logr.Discard()); err != nil {
		t.Fatalf("applyKafkaNetworkConfig: %v", err)
	}
	if !cfg.Net.SASL.Enable {
		t.Error("expected SASL enabled")
	}
	if cfg.Net.TLS.Enable {
		t.Error("expected TLS disabled")
	}
}

func TestApplyKafkaNetworkConfig_ExplicitSASLPlaintext(t *testing.T) {
	cfg := sarama.NewConfig()
	sasl := &v1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "user",
		Password:  "pass",
	}
	if err := applyKafkaNetworkConfig(nil, sasl, "SASL_PLAINTEXT", cfg, logr.Discard()); err != nil {
		t.Fatalf("applyKafkaNetworkConfig: %v", err)
	}
	if !cfg.Net.SASL.Enable {
		t.Error("expected SASL enabled")
	}
	if cfg.Net.TLS.Enable {
		t.Error("expected TLS disabled")
	}
}

func TestApplyKafkaNetworkConfig_ExplicitSASLSSL(t *testing.T) {
	cfg := sarama.NewConfig()
	tls := &v1.TLSConfig{InsecureSkipVerify: true}
	sasl := &v1.SASLConfig{
		Mechanism: "scram-sha-512",
		Username:  "user",
		Password:  "pass",
	}
	if err := applyKafkaNetworkConfig(tls, sasl, "SASL_SSL", cfg, logr.Discard()); err != nil {
		t.Fatalf("applyKafkaNetworkConfig: %v", err)
	}
	if !cfg.Net.SASL.Enable {
		t.Error("expected SASL enabled")
	}
	if !cfg.Net.TLS.Enable {
		t.Error("expected TLS enabled")
	}
}

func TestApplyKafkaNetworkConfig_UnknownProtocol(t *testing.T) {
	cfg := sarama.NewConfig()
	err := applyKafkaNetworkConfig(nil, nil, "WSS", cfg, logr.Discard())
	if err == nil {
		t.Fatal("expected error for unknown security protocol")
	}
}

func TestApplyKafkaNetworkConfig_SASLPlaintextRejectsTLS(t *testing.T) {
	cfg := sarama.NewConfig()
	tls := &v1.TLSConfig{InsecureSkipVerify: true}
	sasl := &v1.SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "user",
		Password:  "pass",
	}
	err := applyKafkaNetworkConfig(tls, sasl, "SASL_PLAINTEXT", cfg, logr.Discard())
	if err == nil {
		t.Fatal("expected error when SASL_PLAINTEXT is combined with tls")
	}
}

func TestApplyKafkaConsumerConfig(t *testing.T) {
	t.Run("maps all fields", func(t *testing.T) {
		minBytes := int32(1)
		maxBytes := int32(1048576)
		partBytes := int32(524288)
		spec := &v1.KafkaSourceSpec{
			ConsumerMaxWait:        &metav1.Duration{Duration: 30 * time.Second},
			FetchMinBytes:          &minBytes,
			FetchMaxBytes:          &maxBytes,
			MaxPartitionFetchBytes: &partBytes,
			NetReadTimeout:         &metav1.Duration{Duration: 60 * time.Second},
			NetWriteTimeout:        &metav1.Duration{Duration: 10 * time.Second},
		}
		cfg := sarama.NewConfig()
		if err := applyKafkaConsumerConfig(spec, cfg); err != nil {
			t.Fatalf("applyKafkaConsumerConfig: %v", err)
		}
		if cfg.Consumer.MaxWaitTime != 30*time.Second {
			t.Errorf("MaxWaitTime = %v, want 30s", cfg.Consumer.MaxWaitTime)
		}
		if cfg.Consumer.Fetch.Min != 1 {
			t.Errorf("Fetch.Min = %d, want 1", cfg.Consumer.Fetch.Min)
		}
		if cfg.Consumer.Fetch.Default != 1048576 {
			t.Errorf("Fetch.Default = %d, want 1048576", cfg.Consumer.Fetch.Default)
		}
		if cfg.Consumer.Fetch.Max != 524288 {
			t.Errorf("Fetch.Max = %d, want 524288", cfg.Consumer.Fetch.Max)
		}
		if cfg.Net.ReadTimeout != 60*time.Second {
			t.Errorf("ReadTimeout = %v, want 60s", cfg.Net.ReadTimeout)
		}
		if cfg.Net.WriteTimeout != 10*time.Second {
			t.Errorf("WriteTimeout = %v, want 10s", cfg.Net.WriteTimeout)
		}
	})

	t.Run("rejects netReadTimeout not greater than consumerMaxWait", func(t *testing.T) {
		spec := &v1.KafkaSourceSpec{
			ConsumerMaxWait: &metav1.Duration{Duration: 30 * time.Second},
			NetReadTimeout:  &metav1.Duration{Duration: 5 * time.Second},
		}
		cfg := sarama.NewConfig()
		if err := applyKafkaConsumerConfig(spec, cfg); err == nil {
			t.Fatal("expected error when netReadTimeout <= consumerMaxWait")
		}
	})
}

func TestApplyKafkaProducerConfig(t *testing.T) {
	t.Run("defaults match historical WaitForAll+idempotent", func(t *testing.T) {
		cfg := sarama.NewConfig()
		if err := applyKafkaProducerConfig(&v1.KafkaSinkSpec{}, cfg); err != nil {
			t.Fatalf("applyKafkaProducerConfig: %v", err)
		}
		if cfg.Producer.RequiredAcks != sarama.WaitForAll {
			t.Errorf("RequiredAcks = %v, want WaitForAll", cfg.Producer.RequiredAcks)
		}
		if !cfg.Producer.Idempotent {
			t.Error("Idempotent = false, want true")
		}
		if cfg.Net.MaxOpenRequests != 1 {
			t.Errorf("MaxOpenRequests = %d, want 1", cfg.Net.MaxOpenRequests)
		}
		if cfg.Producer.Compression != sarama.CompressionNone {
			t.Errorf("Compression = %v, want none", cfg.Producer.Compression)
		}
	})

	t.Run("maps compression flush and async-oriented knobs", func(t *testing.T) {
		flushMsgs := int32(100)
		flushBytes := int32(65536)
		spec := &v1.KafkaSinkSpec{
			Compression:    "snappy",
			RequiredAcks:   "all",
			FlushMessages:  &flushMsgs,
			FlushBytes:     &flushBytes,
			FlushFrequency: &metav1.Duration{Duration: 50 * time.Millisecond},
		}
		cfg := sarama.NewConfig()
		if err := applyKafkaProducerConfig(spec, cfg); err != nil {
			t.Fatalf("applyKafkaProducerConfig: %v", err)
		}
		if cfg.Producer.Compression != sarama.CompressionSnappy {
			t.Errorf("Compression = %v, want snappy", cfg.Producer.Compression)
		}
		if cfg.Producer.Flush.Messages != 100 {
			t.Errorf("Flush.Messages = %d, want 100", cfg.Producer.Flush.Messages)
		}
		if cfg.Producer.Flush.Bytes != 65536 {
			t.Errorf("Flush.Bytes = %d, want 65536", cfg.Producer.Flush.Bytes)
		}
		if cfg.Producer.Flush.Frequency != 50*time.Millisecond {
			t.Errorf("Flush.Frequency = %v, want 50ms", cfg.Producer.Flush.Frequency)
		}
	})

	t.Run("non-idempotent allows local acks and higher maxOpenRequests", func(t *testing.T) {
		idempotent := false
		maxOpen := int32(5)
		spec := &v1.KafkaSinkSpec{
			Idempotent:      &idempotent,
			RequiredAcks:    "local",
			MaxOpenRequests: &maxOpen,
			Compression:     "lz4",
		}
		cfg := sarama.NewConfig()
		if err := applyKafkaProducerConfig(spec, cfg); err != nil {
			t.Fatalf("applyKafkaProducerConfig: %v", err)
		}
		if cfg.Producer.Idempotent {
			t.Error("Idempotent = true, want false")
		}
		if cfg.Producer.RequiredAcks != sarama.WaitForLocal {
			t.Errorf("RequiredAcks = %v, want WaitForLocal", cfg.Producer.RequiredAcks)
		}
		if cfg.Net.MaxOpenRequests != 5 {
			t.Errorf("MaxOpenRequests = %d, want 5", cfg.Net.MaxOpenRequests)
		}
		if cfg.Producer.Compression != sarama.CompressionLZ4 {
			t.Errorf("Compression = %v, want lz4", cfg.Producer.Compression)
		}
	})

	t.Run("rejects idempotent with local acks", func(t *testing.T) {
		cfg := sarama.NewConfig()
		err := applyKafkaProducerConfig(&v1.KafkaSinkSpec{RequiredAcks: "local"}, cfg)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestIsKafkaRequestTimedOutError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"KError", sarama.ErrRequestTimedOut, true},
		{"broker message", errors.New("kafka server: Request exceeded the user-specified time limit in the request"), true},
		{"other", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaRequestTimedOutError(tt.err); got != tt.want {
				t.Errorf("isKafkaRequestTimedOutError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsCoordinatorUnavailableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"coordinator is not available", errors.New("kafka server: The coordinator is not available"), true},
		{"CoordinatorNotAvailable", errors.New("CoordinatorNotAvailable"), true},
		{"wrapped", errors.New("error from consumer: kafka server: The coordinator is not available"), true},
		{"other error", errors.New("connection refused"), false},
		{"other kafka", errors.New("kafka server: Topic not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCoordinatorUnavailableError(tt.err)
			if got != tt.want {
				t.Errorf("isCoordinatorUnavailableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKafkaAuthorizationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"KError topic", sarama.ErrTopicAuthorizationFailed, true},
		{"KError group", sarama.ErrGroupAuthorizationFailed, true},
		{"KError cluster", sarama.ErrClusterAuthorizationFailed, true},
		{"wrapped KError", fmt.Errorf("upstream: %w", sarama.ErrTopicAuthorizationFailed), true},
		{"not authorized string", errors.New("not authorized to access this topic"), true},
		{"TOPIC_AUTHORIZATION_FAILED text", errors.New("TOPIC_AUTHORIZATION_FAILED"), true},
		{"other", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaAuthorizationError(tt.err); got != tt.want {
				t.Errorf("isKafkaAuthorizationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKafkaConsumerGenerationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"KError rebalance", sarama.ErrRebalanceInProgress, true},
		{"KError illegal generation", sarama.ErrIllegalGeneration, true},
		{"KError unknown member", sarama.ErrUnknownMemberId, true},
		{"not known in current generation", errors.New("kafka server: The group member is not known in the current generation"), true},
		{"rebalance in progress text", errors.New("rebalance in progress"), true},
		{"illegal generation text", errors.New("illegal generation"), true},
		{"unknown member id KError", sarama.ErrUnknownMemberId, true},
		{"other", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaConsumerGenerationError(tt.err); got != tt.want {
				t.Errorf("isKafkaConsumerGenerationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKafkaConsumeRetriableError(t *testing.T) {
	authWrapped := fmt.Errorf("wrap: %w", sarama.ErrGroupAuthorizationFailed)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"authorization wrapped", authWrapped, true},
		{"coordinator unavailable", errors.New("kafka server: The coordinator is not available"), true},
		{"i/o timeout", errors.New("read tcp 10.0.0.1:9092->10.0.0.2:9093: i/o timeout"), true},
		{"partition i/o timeout", errors.New("kafka: error while consuming topic/partition/2: read tcp 10.0.0.1:54321->10.0.0.2:9092: i/o timeout"), true},
		{"generation not known", errors.New("kafka server: The group member is not known in the current generation"), true},
		{"rebalance in progress KError", sarama.ErrRebalanceInProgress, true},
		{"illegal generation KError", sarama.ErrIllegalGeneration, true},
		{"context deadline (timeout)", context.DeadlineExceeded, true},
		{"permanent", errors.New("some permanent failure"), false},
		{"unknown topic", errors.New("unknown topic or partition"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaConsumeRetriableError(tt.err); got != tt.want {
				t.Errorf("isKafkaConsumeRetriableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKafkaConsumeRetryReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"authorization", sarama.ErrTopicAuthorizationFailed, "authorization"},
		{"coordinator", errors.New("coordinator is not available"), "coordinator_unavailable"},
		{"timeout", errors.New("read tcp: i/o timeout"), "timeout"},
		{"generation", sarama.ErrRebalanceInProgress, "generation"},
		{"unknown", errors.New("something else"), "transient"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := kafkaConsumeRetryReason(tt.err); got != tt.want {
				t.Errorf("kafkaConsumeRetryReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsKafkaConsumerAsyncRetriableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"i/o timeout", errors.New("read tcp: i/o timeout"), true},
		{"rebalance KError", sarama.ErrRebalanceInProgress, true},
		{"illegal generation", sarama.ErrIllegalGeneration, true},
		{"not known in generation text", errors.New("not known in the current generation"), true},
		{"request timed out (not async-retriable)", errors.New("request exceeded the user-specified time limit"), false},
		{"unknown topic", errors.New("unknown topic or partition"), false},
		{"authorization", sarama.ErrTopicAuthorizationFailed, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaConsumerAsyncRetriableError(tt.err); got != tt.want {
				t.Errorf("isKafkaConsumerAsyncRetriableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKafkaRead_ConsumeRebalanceNilContinues(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return nil // rebalance / session end
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		if res.ch == nil {
			t.Fatal("expected non-nil message channel after rebalance rejoin")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after nil Consume (rebalance) and rejoin")
	}

	if calls < 2 {
		t.Fatalf("Consume calls = %d, want at least 2 (nil rebalance, then successful session)", calls)
	}

	cancel()
}

func TestKafkaRead_MultipleRebalanceNilReturnsBeforeReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls < 3 {
			return nil
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan error, 1)
	go func() {
		_, err := k.Read(ctx)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after multiple nil Consume (rebalance) returns")
	}

	if calls < 3 {
		t.Fatalf("Consume calls = %d, want at least 3 (two nil rebalances, then successful session)", calls)
	}

	cancel()
}

func TestKafkaRead_SessionEndRebalanceCallsConsumeAgain(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	rejoined := make(chan struct{}, 1)
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		if calls == 1 {
			return nil // Sarama: normal session end after rebalance
		}
		select {
		case rejoined <- struct{}{}:
		default:
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh, err := k.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if msgCh == nil {
		t.Fatal("expected non-nil message channel after first session Setup")
	}

	select {
	case <-rejoined:
	case <-time.After(3 * time.Second):
		t.Fatalf("Consume did not rejoin after session-end rebalance (calls=%d)", calls)
	}
	if calls < 2 {
		t.Fatalf("Consume calls = %d, want >= 2 (session end, then rejoin)", calls)
	}

	cancel()
}

func TestKafkaRead_GenerationErrorRetriesThenReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("kafka server: %w", sarama.ErrRebalanceInProgress)
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		if res.ch == nil {
			t.Fatal("expected non-nil message channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after generation error retry")
	}

	if calls != 2 {
		t.Fatalf("Consume calls = %d, want 2 (one generation error, one success)", calls)
	}

	cancel()
}

func TestKafkaRead_UnknownMemberIdRetriesThenReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("kafka server: %w", sarama.ErrUnknownMemberId)
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		if res.ch == nil {
			t.Fatal("expected non-nil message channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after unknown member id retry")
	}

	if calls != 2 {
		t.Fatalf("Consume calls = %d, want 2 (one generation error, one success)", calls)
	}

	cancel()
}

func TestKafkaRead_AsyncRetriableErrorsDoNotSurfaceOnReadErrors(t *testing.T) {
	asyncErrCh := make(chan error, 4)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: asyncErrCh}

	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		asyncErrCh <- errors.New("read tcp 127.0.0.1:54321->127.0.0.1:9092: i/o timeout")
		asyncErrCh <- fmt.Errorf("consumer: %w", sarama.ErrIllegalGeneration)
		asyncErrCh <- errors.New("kafka server: The group member is not known in the current generation")
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgCh, err := k.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if msgCh == nil {
		t.Fatal("expected non-nil message channel")
	}
	readErrors := k.ReadErrors()
	if readErrors == nil {
		t.Fatal("expected ReadErrors channel after Read")
	}

	select {
	case fatal := <-readErrors:
		t.Fatalf("retriable async consumer errors must not surface on ReadErrors, got: %v", fatal)
	case <-time.After(500 * time.Millisecond):
	}

	cancel()
}

func TestKafkaRead_IOTimeoutRetriesThenReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return errors.New("read tcp 127.0.0.1:54321->127.0.0.1:9092: i/o timeout")
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		if res.ch == nil {
			t.Fatal("expected non-nil message channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after i/o timeout retry")
	}

	if calls != 2 {
		t.Fatalf("Consume calls = %d, want 2 (one timeout, one success)", calls)
	}

	cancel()
}

// mockConsumerGroupSession implements sarama.ConsumerGroupSession for testing.
type mockConsumerGroupSession struct {
	ctx context.Context
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32                  { return nil }
func (m *mockConsumerGroupSession) MemberID() string                            { return "test" }
func (m *mockConsumerGroupSession) GenerationID() int32                         { return 1 }
func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string)     {}
func (m *mockConsumerGroupSession) Commit()                                     {}
func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string)    {}
func (m *mockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (m *mockConsumerGroupSession) Context() context.Context                    { return m.ctx }

// mockConsumerGroupClaim implements sarama.ConsumerGroupClaim for testing.
type mockConsumerGroupClaim struct {
	ch chan *sarama.ConsumerMessage
}

func (m *mockConsumerGroupClaim) Topic() string                            { return "test-topic" }
func (m *mockConsumerGroupClaim) Partition() int32                         { return 0 }
func (m *mockConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64               { return 0 }
func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return m.ch }

func TestConsumeClaim_SetsTimestampMetadata(t *testing.T) {
	kafkaTimestamp := time.Date(2024, 2, 27, 10, 13, 20, 0, time.UTC)

	claim := &mockConsumerGroupClaim{
		ch: make(chan *sarama.ConsumerMessage, 1),
	}
	claim.ch <- &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 3,
		Offset:    42,
		Key:       []byte("key-1"),
		Value:     []byte(`{"id":1}`),
		Timestamp: kafkaTimestamp,
	}
	close(claim.ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &mockConsumerGroupSession{ctx: ctx}

	msgChan := make(chan *types.Message, 1)
	handler := &kafkaConsumerGroupHandler{
		connector: &KafkaSourceConnector{
			config:          &v1.KafkaSourceSpec{},
			connectorLogger: connectorLogger{logger: logr.Discard()},
		},
		msgChan:   msgChan,
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	err := handler.ConsumeClaim(session, claim)
	if err != nil {
		t.Fatalf("ConsumeClaim returned error: %v", err)
	}

	select {
	case msg := <-msgChan:
		ts, ok := msg.Metadata["timestamp"].(time.Time)
		if !ok {
			t.Fatal("timestamp not found in metadata or not a time.Time")
		}
		want := time.Date(2024, 2, 27, 10, 13, 20, 0, time.UTC)
		if !ts.Equal(want) {
			t.Errorf("timestamp = %v, want %v", ts, want)
		}

		if topic, _ := msg.Metadata["topic"].(string); topic != "test-topic" {
			t.Errorf("topic = %q, want %q", topic, "test-topic")
		}
		if partition, _ := msg.Metadata["partition"].(int32); partition != 3 {
			t.Errorf("partition = %v, want 3", partition)
		}
		if offset, _ := msg.Metadata["offset"].(int64); offset != 42 {
			t.Errorf("offset = %v, want 42", offset)
		}
		if key, _ := msg.Metadata["key"].(string); key != "key-1" {
			t.Errorf("key = %q, want %q", key, "key-1")
		}
	default:
		t.Fatal("no message received from msgChan")
	}
}

func TestConsumeClaim_SetsHeadersMetadata(t *testing.T) {
	claim := &mockConsumerGroupClaim{
		ch: make(chan *sarama.ConsumerMessage, 1),
	}
	claim.ch <- &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    7,
		Key:       []byte("key-1"),
		Value:     []byte(`{"id":1}`),
		Timestamp: time.Date(2024, 2, 27, 10, 13, 20, 0, time.UTC),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("X-Request-Id"), Value: []byte("req-123")},
			{Key: []byte("X-User-Id"), Value: []byte("user-456")},
		},
	}
	close(claim.ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &mockConsumerGroupSession{ctx: ctx}

	msgChan := make(chan *types.Message, 1)
	handler := &kafkaConsumerGroupHandler{
		connector: &KafkaSourceConnector{
			config:          &v1.KafkaSourceSpec{},
			connectorLogger: connectorLogger{logger: logr.Discard()},
		},
		msgChan:   msgChan,
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	err := handler.ConsumeClaim(session, claim)
	if err != nil {
		t.Fatalf("ConsumeClaim returned error: %v", err)
	}

	select {
	case msg := <-msgChan:
		headers, ok := msg.Metadata["headers"].(map[string]string)
		if !ok {
			t.Fatalf("headers not found in metadata or wrong type: %T", msg.Metadata["headers"])
		}
		if headers["X-Request-Id"] != "req-123" {
			t.Errorf("X-Request-Id = %q, want %q", headers["X-Request-Id"], "req-123")
		}
		if headers["X-User-Id"] != "user-456" {
			t.Errorf("X-User-Id = %q, want %q", headers["X-User-Id"], "user-456")
		}
	default:
		t.Fatal("no message received from msgChan")
	}
}

func TestConsumeClaim_TimestampMetadataUTC(t *testing.T) {
	tests := []struct {
		name      string
		timestamp time.Time
		want      time.Time
	}{
		{
			name:      "zero millis",
			timestamp: time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC),
			want:      time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC),
		},
		{
			name:      "with millis",
			timestamp: time.Date(2024, 6, 1, 12, 30, 45, 123000000, time.UTC),
			want:      time.Date(2024, 6, 1, 12, 30, 45, 123000000, time.UTC),
		},
		{
			name:      "non-UTC converted to UTC",
			timestamp: time.Date(2024, 3, 10, 15, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			want:      time.Date(2024, 3, 10, 20, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := &mockConsumerGroupClaim{
				ch: make(chan *sarama.ConsumerMessage, 1),
			}
			claim.ch <- &sarama.ConsumerMessage{
				Value:     []byte(`{}`),
				Timestamp: tt.timestamp,
			}
			close(claim.ch)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			session := &mockConsumerGroupSession{ctx: ctx}

			msgChan := make(chan *types.Message, 1)
			handler := &kafkaConsumerGroupHandler{
				connector: &KafkaSourceConnector{
					config:          &v1.KafkaSourceSpec{},
					connectorLogger: connectorLogger{logger: logr.Discard()},
				},
				msgChan:   msgChan,
				ready:     make(chan bool),
				readyOnce: sync.Once{},
			}

			if err := handler.ConsumeClaim(session, claim); err != nil {
				t.Fatalf("ConsumeClaim returned error: %v", err)
			}

			msg := <-msgChan
			ts, ok := msg.Metadata["timestamp"].(time.Time)
			if !ok {
				t.Fatal("timestamp not found in metadata or not a time.Time")
			}
			if !ts.Equal(tt.want) {
				t.Errorf("timestamp = %v, want %v", ts, tt.want)
			}
		})
	}
}

func TestKafkaSetup_NotifiesProgress(t *testing.T) {
	t.Parallel()

	var progressCalls atomic.Int32
	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.SetProgressCallback(func() {
		progressCalls.Add(1)
	})

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   make(chan *types.Message),
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := handler.Setup(&mockConsumerGroupSession{ctx: ctx}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if progressCalls.Load() != 1 {
		t.Fatalf("progressCalls = %d, want 1", progressCalls.Load())
	}
}

func TestKafkaConsumeClaim_ProgressHeartbeat(t *testing.T) {
	t.Parallel()

	var progressCalls atomic.Int32
	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.progressHeartbeatInterval = 20 * time.Millisecond
	k.SetProgressCallback(func() {
		progressCalls.Add(1)
	})

	claim := &mockConsumerGroupClaim{ch: make(chan *sarama.ConsumerMessage)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   make(chan *types.Message, 1),
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	done := make(chan error, 1)
	go func() {
		done <- handler.ConsumeClaim(&mockConsumerGroupSession{ctx: ctx}, claim)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for progressCalls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ConsumeClaim")
	}

	if progressCalls.Load() < 1 {
		t.Fatalf("progressCalls = %d, want at least 1", progressCalls.Load())
	}
}

type countingConsumerGroupSession struct {
	mockConsumerGroupSession
	markCount   atomic.Int32
	commitCount atomic.Int32
}

func (s *countingConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {
	s.markCount.Add(1)
}

func (s *countingConsumerGroupSession) Commit() {
	s.commitCount.Add(1)
}

func waitForAckMarks(t *testing.T, session *countingConsumerGroupSession, wantMarks int32, wantCommits int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if session.markCount.Load() == wantMarks && session.commitCount.Load() == wantCommits {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("markCount = %d, want %d; commitCount = %d, want %d",
		session.markCount.Load(), wantMarks, session.commitCount.Load(), wantCommits)
}

func TestConsumeClaim_AckGranularity_MessageCommitsOffsets(t *testing.T) {
	t.Parallel()

	claim := &mockConsumerGroupClaim{
		ch: make(chan *sarama.ConsumerMessage, 1),
	}
	consumerMsg := &sarama.ConsumerMessage{Value: []byte(`{}`), Offset: 7}
	claim.ch <- consumerMsg

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &countingConsumerGroupSession{mockConsumerGroupSession: mockConsumerGroupSession{ctx: ctx}}
	msgChan := make(chan *types.Message, 1)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.SetAckGranularity(v1.AckGranularityMessage)

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   msgChan,
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	done := make(chan error, 1)
	go func() {
		done <- handler.ConsumeClaim(session, claim)
	}()

	var msg *types.Message
	select {
	case msg = <-msgChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
	if msg.Ack == nil {
		t.Fatal("expected Ack callback")
	}
	msg.Ack()
	waitForAckMarks(t, session, 1, 1)

	close(claim.ch)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ConsumeClaim exit")
	}
}

func TestConsumeClaim_AckGranularity_BatchDoesNotCommitImmediately(t *testing.T) {
	t.Parallel()

	claim := &mockConsumerGroupClaim{
		ch: make(chan *sarama.ConsumerMessage, 1),
	}
	claim.ch <- &sarama.ConsumerMessage{Value: []byte(`{}`)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &countingConsumerGroupSession{mockConsumerGroupSession: mockConsumerGroupSession{ctx: ctx}}
	msgChan := make(chan *types.Message, 1)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.SetAckGranularity(v1.AckGranularityBatch)

	handler := &kafkaConsumerGroupHandler{
		connector: k,
		msgChan:   msgChan,
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	done := make(chan error, 1)
	go func() {
		done <- handler.ConsumeClaim(session, claim)
	}()

	var msg *types.Message
	select {
	case msg = <-msgChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
	msg.Ack()
	waitForAckMarks(t, session, 1, 0)

	close(claim.ch)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ConsumeClaim exit")
	}
}
