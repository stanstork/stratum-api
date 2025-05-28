package main

import (
	"log"
	"net/http"

	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/migration"
	"github.com/stanstork/stratum-api/internal/routes"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Run database migrations
	migration.RunMigrations(cfg.DatabaseURL)

	router := routes.NewRouter()

	addr := ":" + cfg.ServerPort
	log.Printf("Server listening on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
