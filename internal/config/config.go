package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	App      AppConfig      `mapstructure:"app"`
	GRPC     GRPCConfig     `mapstructure:"grpc"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
	Chat     ChatConfig     `mapstructure:"chat"`
	Log      LogConfig      `mapstructure:"log"`
}

type AppConfig struct {
	Name        string `mapstructure:"name"`
	Version     string `mapstructure:"version"`
	Environment string `mapstructure:"environment"`
}

type GRPCConfig struct {
	Host              string        `mapstructure:"host"`
	Port              int           `mapstructure:"port"`
	MaxConnectionIdle time.Duration `mapstructure:"max_connection_idle"`
	MaxConnectionAge  time.Duration `mapstructure:"max_connection_age"`
	Timeout           time.Duration `mapstructure:"timeout"`
	MaxRetries        int           `mapstructure:"max_retries"`
	RetryWaitMin      time.Duration `mapstructure:"retry_wait_min"`
	RetryWaitMax      time.Duration `mapstructure:"retry_wait_max"`
}

func (g GRPCConfig) Address() string {
	return fmt.Sprintf("%s:%d", g.Host, g.Port)
}

type RabbitMQConfig struct {
	URL            string        `mapstructure:"url"`
	Exchange       string        `mapstructure:"exchange"`
	ExchangeType   string        `mapstructure:"exchange_type"`
	Queue          string        `mapstructure:"queue"`
	DLQQueue       string        `mapstructure:"dlq_queue"`
	RoutingKey     string        `mapstructure:"routing_key"`
	ConsumerTag    string        `mapstructure:"consumer_tag"`
	Prefetch       int           `mapstructure:"prefetch"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
}

type ChatConfig struct {
	Host              string        `mapstructure:"host"`
	Port              int           `mapstructure:"port"`
	MaxClients        int           `mapstructure:"max_clients"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	MessageHistory    int           `mapstructure:"message_history"`
}

func (c ChatConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load reads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Defaults
	setDefaults(v)

	// Config file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	if configPath != "" {
		v.AddConfigPath(configPath)
	}
	v.AddConfigPath("./configs")
	v.AddConfigPath(".")

	// Environment variables
	v.SetEnvPrefix("DS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "distributed-systems")
	v.SetDefault("app.version", "1.0.0")
	v.SetDefault("app.environment", "development")

	v.SetDefault("grpc.host", "0.0.0.0")
	v.SetDefault("grpc.port", 50051)
	v.SetDefault("grpc.max_connection_idle", "5m")
	v.SetDefault("grpc.max_connection_age", "10m")
	v.SetDefault("grpc.timeout", "30s")
	v.SetDefault("grpc.max_retries", 3)
	v.SetDefault("grpc.retry_wait_min", "1s")
	v.SetDefault("grpc.retry_wait_max", "5s")

	v.SetDefault("rabbitmq.url", "amqp://guest:guest@localhost:5672/")
	v.SetDefault("rabbitmq.exchange", "calculator_events")
	v.SetDefault("rabbitmq.exchange_type", "topic")
	v.SetDefault("rabbitmq.queue", "calculation_results")
	v.SetDefault("rabbitmq.dlq_queue", "calculation_results_dlq")
	v.SetDefault("rabbitmq.routing_key", "calculation.result")
	v.SetDefault("rabbitmq.consumer_tag", "worker-1")
	v.SetDefault("rabbitmq.prefetch", 10)
	v.SetDefault("rabbitmq.reconnect_delay", "5s")

	v.SetDefault("chat.host", "0.0.0.0")
	v.SetDefault("chat.port", 8080)
	v.SetDefault("chat.max_clients", 1000)
	v.SetDefault("chat.read_timeout", "60s")
	v.SetDefault("chat.write_timeout", "10s")
	v.SetDefault("chat.heartbeat_interval", "30s")
	v.SetDefault("chat.message_history", 100)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}
