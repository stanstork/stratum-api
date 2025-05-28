package migration

import (
	"database/sql"
	"embed"
	"log"

	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
)

// Embed SQL files from the local migrations folder
//
//go:embed migrations/*.sql
var embeddedMigrations embed.FS

func RunMigrations(dbUrl string) {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Ensure the tenant schema exists before running migrations
	if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS tenant"); err != nil {
		log.Fatalf("failed to create schema tenant: %v", err)
	}

	// Set the search path to the tenant schema
	if _, err := db.Exec("SET search_path TO tenant"); err != nil {
		log.Fatalf("Failed to set search path: %v", err)
	}

	goose.SetBaseFS(embeddedMigrations)
	goose.SetTableName("tenant.goose_db_version")

	if err := goose.Up(db, "migrations"); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	log.Println("Migrations completed successfully")
}
