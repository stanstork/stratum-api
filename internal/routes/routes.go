package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/handlers"
)

// RegisterRoutes sets up the API routes
func NewRouter(auth *handlers.AuthHandler) *mux.Router {
	router := mux.NewRouter()

	// Health check route
	router.HandleFunc("/health", handlers.HealthCheck).Methods(http.MethodGet)

	// Public auth endpoints
	router.HandleFunc("/api/signup", auth.SignUp).Methods(http.MethodPost)
	router.HandleFunc("/api/login", auth.Login).Methods(http.MethodPost)

	return router
}
