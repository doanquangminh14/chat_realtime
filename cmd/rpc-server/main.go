package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	"github.com/distributed-systems/internal/messaging/broker"
	"github.com/distributed-systems/internal/messaging/producer"
	"github.com/distributed-systems/internal/middleware"
	"github.com/distributed-systems/internal/rpc/handler"
	calculatorpb "github.com/distributed-systems/internal/rpc/proto"
	"github.com/distributed-systems/internal/rpc/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log, err := logger.New(cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("starting gRPC server",
		zap.String("version", cfg.App.Version),
		zap.String("env", cfg.App.Environment),
		zap.String("address", cfg.GRPC.Address()),
	)

	// Setup RabbitMQ (optional — server works without it)
	var eventPublisher service.EventPublisher
	rmqBroker, err := broker.NewRabbitMQBroker(cfg.RabbitMQ, log)
	if err != nil {
		log.Warn("RabbitMQ not available, running without event publishing", zap.Error(err))
	} else {
		setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := rmqBroker.SetupTopology(setupCtx); err != nil {
			log.Warn("RabbitMQ topology setup failed", zap.Error(err))
		}
		cancel()
		eventPublisher = producer.NewCalculationEventProducer(rmqBroker, cfg.RabbitMQ, log)
		defer rmqBroker.Close()
	}

	// Build dependency graph (manual DI)
	calcSvc := service.NewCalculatorService(log, eventPublisher)
	calcHandler := handler.NewCalculatorHandler(calcSvc, log)

	// Build gRPC server with interceptor chain
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: cfg.GRPC.MaxConnectionIdle,
			MaxConnectionAge:  cfg.GRPC.MaxConnectionAge,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
		grpc.ChainUnaryInterceptor(
			middleware.UnaryRecoveryInterceptor(log),
			middleware.UnaryLoggingInterceptor(log),
			middleware.UnaryMetricsInterceptor(log),
		),
	)

	calculatorpb.RegisterCalculatorServiceServer(grpcServer, calcHandler)

	lis, err := net.Listen("tcp", cfg.GRPC.Address())
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	// Graceful shutdown on OS signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Info("gRPC server listening", zap.String("address", cfg.GRPC.Address()))
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("gRPC serve error", zap.Error(err))
		}
	}()

	sig := <-sigCh
	log.Info("shutdown signal received", zap.String("signal", sig.String()))

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("gRPC server stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Warn("graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}
}
