package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/handlers"
)

// RegisterRoutes sets up the API routes
func NewRouter(auth *handlers.AuthHandler, job *handlers.JobHandler, conn *handlers.ConnectionHandler) *mux.Router {
	router := mux.NewRouter()

	// Health check route
	router.HandleFunc("/health", handlers.HealthCheck).Methods(http.MethodGet)

	// Public auth endpoints
	router.HandleFunc("/api/signup", auth.SignUp).Methods(http.MethodPost)
	router.HandleFunc("/api/login", auth.Login).Methods(http.MethodPost)

	// Protected routes with tenant ID in context
	api := router.PathPrefix("/api").Subrouter()
	api.Use(auth.JWTMiddleware)

	// Job management routes
	api.HandleFunc("/jobs", job.CreateJob).Methods(http.MethodPost)
	api.HandleFunc("/jobs", job.ListJobs).Methods(http.MethodGet)
	api.HandleFunc("/jobs/{jobID}/run", job.RunJob).Methods(http.MethodPost)
	api.HandleFunc("/jobs/{jobID}/status", job.GetJobStatus).Methods(http.MethodGet)
	api.HandleFunc("/jobs/executions", job.ListExecutions).Methods(http.MethodGet)
	api.HandleFunc("/jobs/executions/stats", job.GetExecutionStats).Methods(http.MethodGet)

	// Connection management routes
	api.HandleFunc("/connections/test", conn.TestConnection).Methods(http.MethodPost)

	return router
}
