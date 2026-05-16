package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	"github.com/distributed-systems/internal/messaging/broker"
	"github.com/distributed-systems/internal/rpc/service"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	maxRetries  = 3
	retryHeader = "x-retry-count"
)

// MessageHandler processes incoming calculation events
type MessageHandler func(ctx context.Context, event service.CalculationEvent) error

// CalculationEventConsumer consumes calculation events from RabbitMQ
type CalculationEventConsumer struct {
	broker  *broker.RabbitMQBroker
	cfg     config.RabbitMQConfig
	log     *logger.Logger
	handler MessageHandler
}

// NewCalculationEventConsumer creates a new consumer
func NewCalculationEventConsumer(
	b *broker.RabbitMQBroker,
	cfg config.RabbitMQConfig,
	log *logger.Logger,
	handler MessageHandler,
) *CalculationEventConsumer {
	return &CalculationEventConsumer{
		broker:  b,
		cfg:     cfg,
		log:     log.WithComponent("event-consumer"),
		handler: handler,
	}
}

// Start begins consuming messages concurrently with numWorkers goroutines
func (c *CalculationEventConsumer) Start(ctx context.Context, numWorkers int) error {
	ch, err := c.broker.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Prefetch limit for fair dispatch
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		c.cfg.Queue,
		c.cfg.ConsumerTag,
		false, // auto-ack: false — we ack manually
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	c.log.Info("consumer started",
		zap.String("queue", c.cfg.Queue),
		zap.Int("workers", numWorkers),
	)

	// Dispatch to worker goroutines via shared delivery channel
	for i := 0; i < numWorkers; i++ {
		go c.worker(ctx, i, deliveries)
	}

	// Block until context is cancelled, then clean up
	<-ctx.Done()
	c.log.Info("consumer shutting down")
	ch.Close()
	return nil
}

// worker processes deliveries from the shared channel
func (c *CalculationEventConsumer) worker(ctx context.Context, id int, deliveries <-chan amqp.Delivery) {
	workerLog := c.log.With(zap.Int("worker_id", id))
	workerLog.Info("worker started")

	for {
		select {
		case <-ctx.Done():
			workerLog.Info("worker stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				workerLog.Warn("delivery channel closed")
				return
			}
			c.processDelivery(ctx, workerLog, d)
		}
	}
}

// processDelivery unmarshals and handles a single message
func (c *CalculationEventConsumer) processDelivery(ctx context.Context, log *logger.Logger, d amqp.Delivery) {
	var event service.CalculationEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Error("failed to unmarshal message, sending to DLQ",
			zap.Error(err),
			zap.ByteString("body", d.Body),
		)
		_ = d.Nack(false, false) // don't requeue — poison message
		return
	}

	log.Info("processing event",
		zap.String("request_id", event.RequestID),
		zap.String("operation", event.Operation),
		zap.Float64("result", event.Result),
	)

	startTime := time.Now()
	if err := c.handler(ctx, event); err != nil {
		retryCount := c.getRetryCount(d)
		log.Warn("handler failed",
			zap.String("request_id", event.RequestID),
			zap.Int("retry_count", retryCount),
			zap.Error(err),
		)

		if retryCount >= maxRetries {
			log.Error("max retries reached, sending to DLQ",
				zap.String("request_id", event.RequestID),
			)
			_ = d.Nack(false, false)
		} else {
			// Requeue with exponential backoff simulation
			time.Sleep(time.Duration(retryCount+1) * time.Second)
			_ = d.Nack(false, true) // requeue
		}
		return
	}

	log.Info("event processed successfully",
		zap.String("request_id", event.RequestID),
		zap.Duration("processing_time", time.Since(startTime)),
	)
	_ = d.Ack(false)
}

func (c *CalculationEventConsumer) getRetryCount(d amqp.Delivery) int {
	if d.Headers == nil {
		return 0
	}
	if v, ok := d.Headers[retryHeader]; ok {
		if count, ok := v.(int32); ok {
			return int(count)
		}
	}
	return 0
}
