package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

type JobHandler struct {
	repo repository.JobRepository
}

func NewJobHandler(repo repository.JobRepository) *JobHandler {
	return &JobHandler{
		repo: repo,
	}
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	tid := r.Context().Value("tenant_id").(string)
	var payload struct {
		Name                  string          `json:"name"`
		AST                   json.RawMessage `json:"ast"`
		SourceConnection      string          `json:"source_connection"`
		DestinationConnection string          `json:"destination_connection"`
		EngineSettings        string          `json:"engine_settings"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	definition := models.JobDefinition{
		TenantID:              tid,
		Name:                  payload.Name,
		AST:                   payload.AST,
		SourceConnection:      payload.SourceConnection,
		DestinationConnection: payload.DestinationConnection,
		EngineSettings:        payload.EngineSettings,
	}
	createdDef, err := h.repo.CrateDefinition(definition)
	if err != nil {
		http.Error(w, "Failed to create job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createdDef)
}

func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	tid := r.Context().Value("tenant_id").(string)
	definitions, err := h.repo.ListDefinitions(tid)
	if err != nil {
		http.Error(w, "Failed to list job definitions: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(definitions)
}

func (h *JobHandler) RunJob(w http.ResponseWriter, r *http.Request) {
	// tid := r.Context().Value("tenant_id").(string)
	jobDefID := mux.Vars(r)["jobID"]
	// TODO: Verify that the job definition belongs to the tenant
	execution, err := h.repo.CreateExecution(jobDefID)
	if err != nil {
		http.Error(w, "Failed to create job execution: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(execution)
}

func (h *JobHandler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	// tid := r.Context().Value("tenant_id").(string)
	jobDefID := mux.Vars(r)["jobID"]
	// TODO: Verify that the job definition belongs to the tenant
	execution, err := h.repo.GetLastExecution(jobDefID)
	if err != nil {
		http.Error(w, "Failed to get job execution status: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(execution)
}
