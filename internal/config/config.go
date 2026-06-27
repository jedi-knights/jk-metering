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

// Config is the root configuration tree for the worker binary.
type Config struct {
	Log      LogConfig      `mapstructure:"log"`
	Audit    AuditConfig    `mapstructure:"audit"`
	Lago     LagoConfig     `mapstructure:"lago"`
	Metering MeteringConfig `mapstructure:"metering"`
}

// IngestConfig is the root configuration tree for the ingest binary.
// The worker and ingest binaries share the audit DSN and logging
// configuration but have disjoint other concerns: worker pushes to
// Lago, ingest accepts HTTP requests and verifies JWTs.
type IngestConfig struct {
	Log    LogConfig          `mapstructure:"log"`
	Audit  AuditConfig        `mapstructure:"audit"`
	Ingest IngestServerConfig `mapstructure:"ingest"`
}

// IngestServerConfig holds the ingest HTTP server's knobs.
type IngestServerConfig struct {
	// ListenAddr is the bind address for the HTTP server. Default ":8090".
	ListenAddr string `mapstructure:"listen_addr"` // METERING_INGEST_LISTEN_ADDR

	// JWKSURL is the URL where the identity-platform-go JWKS document
	// is published (e.g. https://auth-server.internal/.well-known/jwks.json).
	// Required. Sourced from METERING_INGEST_JWKS_URL.
	JWKSURL string `mapstructure:"jwks_url"`

	// ExpectedIssuer is the iss claim value every accepted token must
	// carry. When empty, issuer is not enforced (development only).
	// Sourced from METERING_INGEST_EXPECTED_ISSUER.
	ExpectedIssuer string `mapstructure:"expected_issuer"`

	// ServiceName is stamped on Event.Service for every emitted event.
	// Default "jk-metering-ingest".
	ServiceName string `mapstructure:"service_name"`
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

// LoadIngest loads the ingest-binary configuration. The audit DSN is
// shared with the worker so both processes write to / read from the
// same audit_events table; the ingest-specific fields configure the
// HTTP server, JWKS source, and audit identity.
func LoadIngest() (*IngestConfig, error) {
	v := viper.New()

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.environment", "development")
	v.SetDefault("audit.dsn", "")
	v.SetDefault("audit.table", "audit_events")
	v.SetDefault("ingest.listen_addr", ":8090")
	v.SetDefault("ingest.jwks_url", "")
	v.SetDefault("ingest.expected_issuer", "")
	v.SetDefault("ingest.service_name", "jk-metering-ingest")

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

	var cfg IngestConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	if cfg.Audit.DSN == "" {
		return nil, fmt.Errorf("validating config: METERING_AUDIT_DSN is required")
	}
	if cfg.Ingest.JWKSURL == "" {
		return nil, fmt.Errorf("validating config: METERING_INGEST_JWKS_URL is required")
	}
	return &cfg, nil
}
