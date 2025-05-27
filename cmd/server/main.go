package main

import (
	"log"
	"net/http"

	"github.com/stanstork/stratum-api/internal/routes"
)

func main() {
	router := routes.NewRouter()

	// Start the HTTP server
	addr := ":8080"
	log.Printf("Starting server on %s...", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
