package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/handlers"
)

// RegisterRoutes sets up the API routes
func NewRouter(auth *handlers.AuthHandler, job *handlers.JobHandler, conn *handlers.ConnectionHandler, meta *handlers.MetadataHandler) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)

	// Health check route
	router.HandleFunc("/health", handlers.HealthCheck).Methods(http.MethodGet)

	// Public auth endpoints
	router.HandleFunc("/api/signup", auth.SignUp).Methods(http.MethodPost)
	router.HandleFunc("/api/login", auth.Login).Methods(http.MethodPost)

	// Protected routes with tenant ID in context
	api := router.PathPrefix("/api").Subrouter()
	api.Use(auth.JWTMiddleware)

	// Base "/jobs" routes
	api.HandleFunc("/jobs", job.CreateJob).Methods(http.MethodPost)
	api.HandleFunc("/jobs", job.ListJobs).Methods(http.MethodGet)

	// Specific sub-paths of "/jobs/..." MUST come BEFORE dynamic "/jobs/{jobID}"

	// Most specific "/jobs/executions/..." route first
	api.HandleFunc("/jobs/executions/stats", job.GetExecutionStats).Methods(http.MethodGet)

	// Parent "/jobs/executions" route next
	api.HandleFunc("/jobs/executions", job.ListExecutions).Methods(http.MethodGet)
	api.HandleFunc("/jobs/executions/{execID}", job.GetExecution).Methods(http.MethodGet)
	api.HandleFunc("/jobs/executions/{execID}/complete", job.SetExecutionComplete).Methods(http.MethodPost)

	api.HandleFunc("/jobs/{jobID}", job.DelteJob).Methods(http.MethodDelete)
	api.HandleFunc("/jobs/{jobID}", job.GetJobDefinition).Methods(http.MethodGet)
	api.HandleFunc("/jobs/{jobID}/run", job.RunJob).Methods(http.MethodPost)
	api.HandleFunc("/jobs/{jobID}/status", job.GetJobStatus).Methods(http.MethodGet)

	// Connection management routes
	api.HandleFunc("/connections/test", conn.TestConnection).Methods(http.MethodPost)
	api.HandleFunc("/connections/{id}/test", conn.TestConnectionByID).Methods(http.MethodPost)
	api.HandleFunc("/connections", conn.List).Methods(http.MethodGet)
	api.HandleFunc("/connections", conn.Create).Methods(http.MethodPost)
	api.HandleFunc("/connections/{id}", conn.Get).Methods(http.MethodGet)
	api.HandleFunc("/connections/{id}", conn.Update).Methods(http.MethodPut)
	api.HandleFunc("/connections/{id}", conn.Delete).Methods(http.MethodDelete)

	// Metadata routes
	api.HandleFunc("/connections/{id}/metadata", meta.GetSourceMetadata).Methods(http.MethodGet)

	return router
}
