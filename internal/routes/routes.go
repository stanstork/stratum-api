package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/handlers"
)

// RegisterRoutes sets up the API routes
func NewRouter() *mux.Router {
	router := mux.NewRouter()

	// Health check route
	router.HandleFunc("/health", handlers.HealthCheck).Methods(http.MethodGet)

	return router
}
