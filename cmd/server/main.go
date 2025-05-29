package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/handlers"
	"github.com/stanstork/stratum-api/internal/migration"
	"github.com/stanstork/stratum-api/internal/routes"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Open DB & run migrations
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open error: %v", err)
	}
	migration.RunMigrations(cfg.DatabaseURL)

	// Init auth handler
	authHandler := handlers.NewAuthHandler(db, cfg)

	// Build the router
	router := routes.NewRouter(authHandler)

	// Start HTTP server
	addr := ":" + cfg.ServerPort
	log.Printf("Server listening on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
