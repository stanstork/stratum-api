package config

import (
	"log"
	"time"

	"github.com/spf13/viper"
)

type WorkerConfig struct {
	PollInterval         time.Duration `mapstructure:"poll_interval"`
	EngineImage          string        `mapstructure:"engine_image"`
	EngineContainer      string        `mapstructure:"engine_container"`
	TempDir              string        `mapstructure:"temp_dir"`
	ContainerCPULimit    int64         `mapstructure:"container_cpu_limit"`
	ContainerMemoryLimit int64         `mapstructure:"container_memory_limit"`
}

type Config struct {
	DatabaseURL string       `mapstructure:"database_url"`
	ServerPort  string       `mapstructure:"server_port"`
	JWTSecret   string       `mapstructure:"jwt_secret"`
	Worker      WorkerConfig `mapstructure:"worker"`
}

// Load reads the configuration from a YAML file and returns a Config instance.
func Load() *Config {
	v := viper.New()

	// Look for config in the current directory and ./config
	v.AddConfigPath(".")
	v.SetConfigName("config")
	v.AddConfigPath("./config")
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		log.Fatalf("Error unmarshalling config: %v", err)
	}

	// Fallback defaults
	if config.ServerPort == "" {
		config.ServerPort = "8080"
	}

	if config.JWTSecret == "" {
		log.Fatal("JWT secret must be set in the config file")
	}

	return &config
}
