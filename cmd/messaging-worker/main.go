package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	"github.com/distributed-systems/internal/messaging/broker"
	"github.com/distributed-systems/internal/messaging/consumer"
	"github.com/distributed-systems/internal/rpc/service"
	"go.uber.org/zap"
)

const numWorkers = 3

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log, err := logger.New(cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("starting messaging worker",
		zap.Int("workers", numWorkers),
		zap.String("queue", cfg.RabbitMQ.Queue),
	)

	rmqBroker, err := broker.NewRabbitMQBroker(cfg.RabbitMQ, log)
	if err != nil {
		log.Fatal("failed to connect to RabbitMQ", zap.Error(err))
	}
	defer rmqBroker.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Setup topology (idempotent)
	setupCtx, setupCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := rmqBroker.SetupTopology(setupCtx); err != nil {
		log.Fatal("topology setup failed", zap.Error(err))
	}
	setupCancel()

	// Create consumer with handler
	h := buildHandler(log)
	c := consumer.NewCalculationEventConsumer(rmqBroker, cfg.RabbitMQ, log, h)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("shutdown signal received", zap.String("signal", sig.String()))
		cancel()
	}()

	// Start blocks until context is cancelled
	if err := c.Start(ctx, numWorkers); err != nil {
		log.Fatal("consumer error", zap.Error(err))
	}

	log.Info("messaging worker stopped")
}

// buildHandler returns the calculation event processing function.
func buildHandler(log *logger.Logger) consumer.MessageHandler {
	handlerLog := log.WithComponent("event-handler")

	return func(ctx context.Context, event service.CalculationEvent) error {
		// Structured log as persistent audit record
		handlerLog.Info("calculation event received",
			zap.String("request_id", event.RequestID),
			zap.String("operation", event.Operation),
			zap.Float64s("operands", event.Operands),
			zap.Float64("result", event.Result),
			zap.String("status", event.Status),
			zap.Time("timestamp", event.Timestamp),
			zap.Int64("computation_ns", event.ComputationTimeNs),
		)

		if event.Status == "error" {
			handlerLog.Warn("calculation failure logged",
				zap.String("request_id", event.RequestID),
				zap.String("error", event.ErrorMessage),
			)
		}

		// Extension point: persist to DB, update dashboards, trigger alerts
		return nil
	}
}
