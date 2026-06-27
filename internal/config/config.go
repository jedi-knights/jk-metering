// Package config loads the metering shim's runtime configuration via
// viper. All variables are namespaced under METERING_ at the env layer;
// the in-process mapping uses dotted keys (e.g. METERING_LAGO_API_KEY →
// lago.api_key) so YAML form lines up with environment overrides.
package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config is the root configuration tree.
type Config struct {
	Log      LogConfig      `mapstructure:"log"`
	Audit    AuditConfig    `mapstructure:"audit"`
	Lago     LagoConfig     `mapstructure:"lago"`
	Metering MeteringConfig `mapstructure:"metering"`
}

// LogConfig holds structured logging configuration.
type LogConfig struct {
	Level       string `mapstructure:"level"`
	Format      string `mapstructure:"format"`
	Environment string `mapstructure:"environment"`
}

// AuditConfig holds the Postgres connection settings for the
// audit_events source.
type AuditConfig struct {
	// DSN is the Postgres connection string for the audit_events table.
	// Required. Sourced from METERING_AUDIT_DSN.
	DSN string `mapstructure:"dsn"`

	// Table overrides the default audit_events table name. Useful when
	// a service writes to a custom audit_events_<service> table. Default
	// "audit_events".
	Table string `mapstructure:"table"`
}

// LagoConfig holds the Lago Event API connection settings.
type LagoConfig struct {
	// BaseURL is the Lago API root (e.g. https://api.getlago.com or
	// http://lago-api.internal). Required. Sourced from METERING_LAGO_BASE_URL.
	BaseURL string `mapstructure:"base_url"`

	// APIKey is the Lago API key. Required. Sourced from
	// METERING_LAGO_API_KEY; never log this value.
	APIKey string `mapstructure:"api_key"`
}

// MeteringConfig holds the shim's operational knobs.
type MeteringConfig struct {
	// PollIntervalSeconds is how often Tick is called. Default 5.
	PollIntervalSeconds int `mapstructure:"poll_interval_seconds"`

	// BatchSize caps the number of events fetched per tick. Default 100.
	BatchSize int `mapstructure:"batch_size"`

	// BillingIdentity selects which audit field becomes the Lago
	// external_subscription_id. One of "subject" (default), "actor",
	// or "client". Other values default to "subject".
	BillingIdentity string `mapstructure:"billing_identity"`
}

// Load reads configuration from environment variables (METERING_*) and
// the optional config.yaml file in the working directory.
func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.environment", "development")
	v.SetDefault("audit.dsn", "")
	v.SetDefault("audit.table", "audit_events")
	v.SetDefault("lago.base_url", "")
	v.SetDefault("lago.api_key", "")
	v.SetDefault("metering.poll_interval_seconds", 5)
	v.SetDefault("metering.batch_size", 100)
	v.SetDefault("metering.billing_identity", "subject")

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")

	v.SetEnvPrefix("METERING")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	if cfg.Audit.DSN == "" {
		return nil, fmt.Errorf("validating config: METERING_AUDIT_DSN is required")
	}
	if cfg.Lago.BaseURL == "" {
		return nil, fmt.Errorf("validating config: METERING_LAGO_BASE_URL is required")
	}
	if cfg.Lago.APIKey == "" {
		return nil, fmt.Errorf("validating config: METERING_LAGO_API_KEY is required")
	}
	return &cfg, nil
}
