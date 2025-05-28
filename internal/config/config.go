package config

import (
	"log"

	"github.com/spf13/viper"
)

// Config holds all configurable values
// Field tags map to YAML keys
type Config struct {
	DatabaseURL string `mapstructure:"database_url"`
	ServerPort  string `mapstructure:"server_port"`
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

	return &config
}
