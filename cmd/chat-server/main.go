package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-systems/internal/chat/manager"
	"github.com/distributed-systems/internal/chat/server"
	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	"go.uber.org/zap"
)

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

	log.Info("starting chat server",
		zap.String("address", cfg.Chat.Address()),
		zap.Int("max_clients", cfg.Chat.MaxClients),
	)

	mgr := manager.NewManager(cfg.Chat.MessageHistory, log)
	chatServer := server.NewChatServer(cfg.Chat, mgr, log)

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("shutdown signal received", zap.String("signal", sig.String()))
		cancel()
	}()

	if err := chatServer.Start(ctx); err != nil {
		log.Fatal("chat server error", zap.Error(err))
	}

	log.Info("chat server stopped")
}
