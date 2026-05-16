package producer

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

// CalculationEventProducer publishes calculation events to RabbitMQ
type CalculationEventProducer struct {
	broker *broker.RabbitMQBroker
	cfg    config.RabbitMQConfig
	log    *logger.Logger
}

// NewCalculationEventProducer creates a new producer
func NewCalculationEventProducer(
	b *broker.RabbitMQBroker,
	cfg config.RabbitMQConfig,
	log *logger.Logger,
) *CalculationEventProducer {
	return &CalculationEventProducer{
		broker: b,
		cfg:    cfg,
		log:    log.WithComponent("event-producer"),
	}
}

// Publish sends a calculation event to the message broker
func (p *CalculationEventProducer) Publish(ctx context.Context, event service.CalculationEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	ch, err := p.broker.Channel()
	if err != nil {
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer ch.Close()

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		MessageId:    event.RequestID,
		Body:         body,
		Headers: amqp.Table{
			"operation": event.Operation,
			"status":    event.Status,
		},
	}

	if err := ch.PublishWithContext(
		ctx,
		p.cfg.Exchange,
		p.cfg.RoutingKey,
		false, // mandatory
		false, // immediate
		msg,
	); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	p.log.Debug("event published",
		zap.String("request_id", event.RequestID),
		zap.String("operation", event.Operation),
		zap.String("routing_key", p.cfg.RoutingKey),
	)

	return nil
}
