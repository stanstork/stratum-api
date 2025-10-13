package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/handlers"
	"github.com/stanstork/stratum-api/internal/models"
)

// RegisterRoutes sets up the API routes
func NewRouter(auth *handlers.AuthHandler,
	job *handlers.JobHandler,
	conn *handlers.ConnectionHandler,
	meta *handlers.MetadataHandler,
	report *handlers.ReportHandler,
	tenant *handlers.TenantHandler,
	invite *handlers.InviteHandler) *mux.Router {

	router := mux.NewRouter().StrictSlash(true)

	// Health check route
	router.HandleFunc("/health", handlers.HealthCheck).Methods(http.MethodGet)

	// Public auth endpoints
	router.HandleFunc("/api/signup", auth.SignUp).Methods(http.MethodPost)
	router.HandleFunc("/api/login", auth.Login).Methods(http.MethodPost)

	// Public invite workflows
	router.HandleFunc("/api/invites/{token}", invite.PreviewInvite).Methods(http.MethodGet)
	router.HandleFunc("/api/invites/{token}/accept", invite.AcceptInvite).Methods(http.MethodPost)

	// Protected routes with tenant ID in context
	api := router.PathPrefix("/api").Subrouter()
	api.Use(auth.JWTMiddleware)

	api.Handle("/tenants",
		authz.RequireRoleHandler(models.RoleSuperAdmin, http.HandlerFunc(tenant.CreateTenant)),
	).Methods(http.MethodPost)
	api.Handle("/tenants/{tenantID}/users",
		authz.RequireRoleHandler(models.RoleAdmin, http.HandlerFunc(tenant.ListUsers)),
	).Methods(http.MethodGet)
	api.Handle("/tenants/{tenantID}/users",
		authz.RequireRoleHandler(models.RoleAdmin, http.HandlerFunc(tenant.AddUser)),
	).Methods(http.MethodPost)
	api.Handle("/tenants/{tenantID}/invites",
		authz.RequireRoleHandler(models.RoleAdmin, http.HandlerFunc(invite.CreateInvite)),
	).Methods(http.MethodPost)
	api.Handle("/users/invites",
		authz.RequireRoleHandler(models.RoleAdmin, http.HandlerFunc(invite.CreateCurrentTenantInvite)),
	).Methods(http.MethodPost)
	api.Handle("/users",
		authz.RequireRoleHandler(models.RoleAdmin, http.HandlerFunc(tenant.ListCurrentTenantUsers)),
	).Methods(http.MethodGet)

	// Base "/jobs" routes
	api.Handle("/jobs",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(job.CreateJob)),
	).Methods(http.MethodPost)
	api.HandleFunc("/jobs", job.ListJobs).Methods(http.MethodGet)

	// Specific sub-paths of "/jobs/..." MUST come BEFORE dynamic "/jobs/{jobID}"

	// Most specific "/jobs/executions/..." route first
	api.HandleFunc("/jobs/executions/stats", job.GetExecutionStats).Methods(http.MethodGet)

	// Parent "/jobs/executions" route next
	api.HandleFunc("/jobs/executions", job.ListExecutions).Methods(http.MethodGet)
	api.HandleFunc("/jobs/executions/{execID}", job.GetExecution).Methods(http.MethodGet)
	api.Handle("/jobs/executions/{execID}/complete",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(job.SetExecutionComplete)),
	).Methods(http.MethodPost)

	api.HandleFunc("/jobs/stats", job.ListJobDefinitionsWithStats).Methods(http.MethodGet)
	api.Handle("/jobs/{jobID}",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(job.DelteJob)),
	).Methods(http.MethodDelete)
	api.HandleFunc("/jobs/{jobID}", job.GetJobDefinition).Methods(http.MethodGet)
	api.Handle("/jobs/{jobID}/run",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(job.RunJob)),
	).Methods(http.MethodPost)
	api.HandleFunc("/jobs/{jobID}/status", job.GetJobStatus).Methods(http.MethodGet)

	// Connection management routes
	api.Handle("/connections/test",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(conn.TestConnection)),
	).Methods(http.MethodPost)
	api.Handle("/connections/{id}/test",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(conn.TestConnectionByID)),
	).Methods(http.MethodPost)
	api.HandleFunc("/connections", conn.List).Methods(http.MethodGet)
	api.Handle("/connections",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(conn.Create)),
	).Methods(http.MethodPost)
	api.HandleFunc("/connections/{id}", conn.Get).Methods(http.MethodGet)
	api.Handle("/connections/{id}",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(conn.Update)),
	).Methods(http.MethodPut)
	api.Handle("/connections/{id}",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(conn.Delete)),
	).Methods(http.MethodDelete)

	// Metadata routes
	api.Handle("/connections/{id}/metadata",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(meta.GetSourceMetadata)),
	).Methods(http.MethodGet)

	// Report routes
	api.Handle("/reports/dry-run/{definition_id}",
		authz.RequireRoleHandler(models.RoleEditor, http.HandlerFunc(report.DryRunReport)),
	).Methods(http.MethodPost)

	return router
}
