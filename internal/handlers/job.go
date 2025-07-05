package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

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
		Name                    string          `json:"name"`
		Description             string          `json:"description"`
		AST                     json.RawMessage `json:"ast"`
		SourceConnectionID      string          `json:"source_connection_id"`
		DestinationConnectionID string          `json:"destination_connection_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	definition := models.JobDefinition{
		TenantID:                tid,
		Name:                    payload.Name,
		Description:             payload.Description,
		AST:                     payload.AST,
		SourceConnectionID:      payload.SourceConnectionID,
		DestinationConnectionID: payload.DestinationConnectionID,
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
	// tid := r.Context().Value("tenant_id").(string)
	definitions, err := h.repo.ListDefinitions()
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

func (h *JobHandler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	// parse query params with defaults
	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil {
			offset = v
		}
	}

	executions, err := h.repo.ListExecutions(limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(executions)
}

func (h *JobHandler) GetExecutionStats(w http.ResponseWriter, r *http.Request) {
	days := 31 // default to 31 days
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			days = v
		}
	}

	stats, err := h.repo.ListExecutionStats(days)
	if err != nil {
		http.Error(w, "Failed to get execution stats: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
