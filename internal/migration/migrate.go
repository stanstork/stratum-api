package migration

import (
	"database/sql"
	"embed"

	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog"
)

// Embed SQL files from the local migrations folder
//
//go:embed migrations/*.sql
var embeddedMigrations embed.FS

type GooseAdapter struct {
	logger zerolog.Logger
}

func NewGooseAdapter(logger zerolog.Logger) *GooseAdapter {
	return &GooseAdapter{
		logger: logger.With().Str("component", "goose").Logger(),
	}
}

// Printf logs a message at the Info level.
// goose's non-fatal logs are generally informational.
func (a *GooseAdapter) Printf(format string, v ...interface{}) {
	a.logger.Info().Msgf(format, v...)
}

// Fatalf logs a message at the Fatal level.
func (a *GooseAdapter) Fatalf(format string, v ...interface{}) {
	a.logger.Fatal().Msgf(format, v...)
}

func RunMigrations(dbUrl string, logger zerolog.Logger) {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to the database for migrations")
	}
	defer db.Close()

	// Ensure the tenant schema exists before running migrations
	if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS tenant"); err != nil {
		logger.Fatal().Err(err).Msg("failed to create schema tenant")
	}

	// Set the search path to the tenant schema
	if _, err := db.Exec("SET search_path TO tenant"); err != nil {
		logger.Fatal().Err(err).Msg("Failed to set search path")
	}

	goose.SetBaseFS(embeddedMigrations)
	goose.SetTableName("tenant.goose_db_version")

	if err := goose.Up(db, "migrations"); err != nil {
		logger.Fatal().Err(err).Msg("Failed to run migrations")
	}

	logger.Info().Msg("Migrations completed successfully")
}
