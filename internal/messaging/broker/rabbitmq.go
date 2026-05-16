package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// RabbitMQBroker manages AMQP connection lifecycle with auto-reconnect
type RabbitMQBroker struct {
	cfg         config.RabbitMQConfig
	log         *logger.Logger
	conn        *amqp.Connection
	mu          sync.RWMutex
	closeCh     chan struct{}
	reconnectCh chan struct{}
}

// NewRabbitMQBroker creates and connects a new broker
func NewRabbitMQBroker(cfg config.RabbitMQConfig, log *logger.Logger) (*RabbitMQBroker, error) {
	b := &RabbitMQBroker{
		cfg:         cfg,
		log:         log.WithComponent("rabbitmq-broker"),
		closeCh:     make(chan struct{}),
		reconnectCh: make(chan struct{}, 1),
	}

	if err := b.connect(); err != nil {
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}

	go b.watchConnection()
	return b, nil
}

// connect establishes the AMQP connection
func (b *RabbitMQBroker) connect() error {
	b.log.Info("connecting to RabbitMQ", zap.String("url", maskPassword(b.cfg.URL)))

	conn, err := amqp.Dial(b.cfg.URL)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()

	b.log.Info("RabbitMQ connection established")

	// Monitor connection closure
	go func() {
		errCh := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case amqpErr := <-errCh:
			if amqpErr != nil {
				b.log.Warn("RabbitMQ connection closed unexpectedly",
					zap.String("reason", amqpErr.Reason),
					zap.Int("code", amqpErr.Code),
				)
				select {
				case b.reconnectCh <- struct{}{}:
				default:
				}
			}
		case <-b.closeCh:
		}
	}()

	return nil
}

// watchConnection handles reconnection logic
func (b *RabbitMQBroker) watchConnection() {
	for {
		select {
		case <-b.closeCh:
			return
		case <-b.reconnectCh:
			b.log.Info("attempting to reconnect to RabbitMQ")
			for {
				time.Sleep(b.cfg.ReconnectDelay)
				if err := b.connect(); err != nil {
					b.log.Error("reconnect failed, retrying", zap.Error(err))
					continue
				}
				break
			}
		}
	}
}

// Channel returns a new AMQP channel from the current connection
func (b *RabbitMQBroker) Channel() (*amqp.Channel, error) {
	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil, fmt.Errorf("connection is not available")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	return ch, nil
}

// SetupTopology declares the exchange and queues
func (b *RabbitMQBroker) SetupTopology(ctx context.Context) error {
	ch, err := b.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Declare main exchange
	if err := ch.ExchangeDeclare(
		b.cfg.Exchange,
		b.cfg.ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}

	// Declare DLQ exchange
	if err := ch.ExchangeDeclare(
		b.cfg.Exchange+".dlx",
		"fanout",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("DLX exchange declare failed: %w", err)
	}

	// Declare DLQ queue
	_, err = ch.QueueDeclare(
		b.cfg.DLQQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("DLQ declare failed: %w", err)
	}

	if err := ch.QueueBind(b.cfg.DLQQueue, "", b.cfg.Exchange+".dlx", false, nil); err != nil {
		return fmt.Errorf("DLQ bind failed: %w", err)
	}

	// Declare main queue with DLX routing
	_, err = ch.QueueDeclare(
		b.cfg.Queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": b.cfg.Exchange + ".dlx",
			"x-message-ttl":          int32(60000), // 60s TTL
		},
	)
	if err != nil {
		return fmt.Errorf("queue declare failed: %w", err)
	}

	if err := ch.QueueBind(b.cfg.Queue, b.cfg.RoutingKey, b.cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("queue bind failed: %w", err)
	}

	b.log.Info("RabbitMQ topology configured",
		zap.String("exchange", b.cfg.Exchange),
		zap.String("queue", b.cfg.Queue),
	)

	return nil
}

// Close gracefully closes the AMQP connection
func (b *RabbitMQBroker) Close() error {
	close(b.closeCh)
	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()

	if conn != nil && !conn.IsClosed() {
		return conn.Close()
	}
	return nil
}

// maskPassword replaces the password in AMQP URL for safe logging
func maskPassword(url string) string {
	// Simple masking — in production use url.Parse
	return "amqp://***:***@..."
}
