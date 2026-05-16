package main

import (
	"fmt"
	"os"

	"github.com/distributed-systems/internal/chat/client"
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

	log, _ := logger.New("info", "console")
	defer log.Sync()

	address := cfg.Chat.Address()
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	log.Info("connecting to chat server", zap.String("address", address))

	chatClient := client.NewChatClient(address, log)
	if err := chatClient.Connect(); err != nil {
		log.Fatal("connection failed", zap.Error(err))
	}

	if err := chatClient.Run(); err != nil {
		log.Error("client error", zap.Error(err))
	}

	chatClient.Close()
	log.Info("disconnected")
}
