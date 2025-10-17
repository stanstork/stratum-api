package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

type JobHandler struct {
	repo repository.JobRepository
}

type createDefinitionPayload struct {
	Name                    string          `json:"name"`
	Description             string          `json:"description"`
	AST                     json.RawMessage `json:"ast"`
	SourceConnectionID      string          `json:"source_connection_id"`
	DestinationConnectionID string          `json:"destination_connection_id"`
	ProgressSnapshot        json.RawMessage `json:"progress_snapshot"`
	Status                  string          `json:"status"`
}

type updateDefinitionPayload struct {
	Name                    *string          `json:"name"`
	Description             *string          `json:"description"`
	AST                     *json.RawMessage `json:"ast"`
	SourceConnectionID      *string          `json:"source_connection_id"`
	DestinationConnectionID *string          `json:"destination_connection_id"`
	ProgressSnapshot        *json.RawMessage `json:"progress_snapshot"`
	Status                  *string          `json:"status"`
}

func (p updateDefinitionPayload) hasChanges() bool {
	return p.Name != nil ||
		p.Description != nil ||
		p.AST != nil ||
		p.SourceConnectionID != nil ||
		p.DestinationConnectionID != nil ||
		p.ProgressSnapshot != nil ||
		p.Status != nil
}

type resolvedDefinition struct {
	Name                    string
	Description             string
	AST                     json.RawMessage
	SourceConnectionID      string
	DestinationConnectionID string
	ProgressSnapshot        json.RawMessage
}

func NewJobHandler(repo repository.JobRepository) *JobHandler {
	return &JobHandler{
		repo: repo,
	}
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	return strings.Contains(err.Error(), "not found")
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	var payload createDefinitionPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	status := strings.ToUpper(strings.TrimSpace(payload.Status))
	if status == "" {
		status = "READY"
	}
	if status == "READY" {
		if len(payload.AST) == 0 {
			http.Error(w, "AST is required when status is READY", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(payload.SourceConnectionID) == "" || strings.TrimSpace(payload.DestinationConnectionID) == "" {
			http.Error(w, "Source and destination connections are required when status is READY", http.StatusBadRequest)
			return
		}
	}
	definition := models.JobDefinition{
		TenantID:                tid,
		Name:                    name,
		Description:             payload.Description,
		AST:                     payload.AST,
		SourceConnectionID:      strings.TrimSpace(payload.SourceConnectionID),
		DestinationConnectionID: strings.TrimSpace(payload.DestinationConnectionID),
		Status:                  status,
		ProgressSnapshot:        cloneRawMessage(payload.ProgressSnapshot),
	}
	createdDef, err := h.repo.CrateDefinition(definition)
	if err != nil {
		http.Error(w, "Failed to create job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createdDef)
}

func (h *JobHandler) CreateDraft(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	var payload createDefinitionPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	definition := models.JobDefinition{
		TenantID:                tid,
		Name:                    name,
		Description:             payload.Description,
		AST:                     payload.AST,
		SourceConnectionID:      strings.TrimSpace(payload.SourceConnectionID),
		DestinationConnectionID: strings.TrimSpace(payload.DestinationConnectionID),
		Status:                  "DRAFT",
		ProgressSnapshot:        cloneRawMessage(payload.ProgressSnapshot),
	}
	createdDef, err := h.repo.CrateDefinition(definition)
	if err != nil {
		http.Error(w, "Failed to create draft job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createdDef)
}

func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	definitions, err := h.repo.ListDefinitions(tid)
	if err != nil {
		http.Error(w, "Failed to list job definitions: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, definitions)
}

func (h *JobHandler) AutosaveJob(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]

	var payload updateDefinitionPayload
	if err := decodeAllowEmpty(r, &payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	currentDef, err := h.repo.GetJobDefinitionByID(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	update := repository.DefinitionUpdate{}

	if payload.Name != nil {
		name := strings.TrimSpace(*payload.Name)
		if name == "" {
			http.Error(w, "Name cannot be empty", http.StatusBadRequest)
			return
		}
		update.Name = &name
	}
	if payload.Description != nil {
		desc := *payload.Description
		update.Description = &desc
	}
	if payload.AST != nil {
		ast := cloneRawMessage(*payload.AST)
		update.AST = &ast
	}
	if payload.SourceConnectionID != nil {
		src := strings.TrimSpace(*payload.SourceConnectionID)
		update.SourceConnectionID = &src
	}
	if payload.DestinationConnectionID != nil {
		dst := strings.TrimSpace(*payload.DestinationConnectionID)
		update.DestinationConnectionID = &dst
	}
	if payload.ProgressSnapshot != nil {
		snapshot := cloneRawMessage(*payload.ProgressSnapshot)
		update.ProgressSnapshot = &snapshot
	}

	if payload.Status != nil {
		status := strings.ToUpper(strings.TrimSpace(*payload.Status))
		update.Status = &status
	} else if currentDef.Status == "READY" && payload.hasChanges() {
		status := "DRAFT"
		update.Status = &status
	}

	updatedDef, err := h.repo.UpdateDefinition(tid, jobDefID, update)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to save definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, updatedDef)
}

func (h *JobHandler) ValidateJobDefinition(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]

	var payload updateDefinitionPayload
	if err := decodeAllowEmpty(r, &payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	currentDef, err := h.repo.GetJobDefinitionByID(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resolved := resolveDefinition(payload, currentDef)
	if errs := validateResolvedDefinition(resolved); len(errs) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"valid":  false,
			"errors": errs,
		})
		return
	}

	update := repository.DefinitionUpdate{}
	name := resolved.Name
	update.Name = &name
	desc := resolved.Description
	update.Description = &desc
	ast := cloneRawMessage(resolved.AST)
	update.AST = &ast
	src := strings.TrimSpace(resolved.SourceConnectionID)
	update.SourceConnectionID = &src
	dst := strings.TrimSpace(resolved.DestinationConnectionID)
	update.DestinationConnectionID = &dst
	status := "VALIDATING"
	update.Status = &status
	if payload.ProgressSnapshot != nil {
		snapshot := cloneRawMessage(*payload.ProgressSnapshot)
		update.ProgressSnapshot = &snapshot
	}

	updatedDef, err := h.repo.UpdateDefinition(tid, jobDefID, update)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to validate definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid":      true,
		"definition": updatedDef,
	})
}

func (h *JobHandler) MarkDefinitionReady(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]

	var payload updateDefinitionPayload
	if err := decodeAllowEmpty(r, &payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	currentDef, err := h.repo.GetJobDefinitionByID(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resolved := resolveDefinition(payload, currentDef)
	if errs := validateResolvedDefinition(resolved); len(errs) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"valid":  false,
			"errors": errs,
		})
		return
	}

	update := repository.DefinitionUpdate{}
	name := resolved.Name
	update.Name = &name
	desc := resolved.Description
	update.Description = &desc
	ast := cloneRawMessage(resolved.AST)
	update.AST = &ast
	src := strings.TrimSpace(resolved.SourceConnectionID)
	update.SourceConnectionID = &src
	dst := strings.TrimSpace(resolved.DestinationConnectionID)
	update.DestinationConnectionID = &dst
	status := "READY"
	update.Status = &status
	if payload.ProgressSnapshot != nil {
		snapshot := cloneRawMessage(*payload.ProgressSnapshot)
		update.ProgressSnapshot = &snapshot
	}

	updatedDef, err := h.repo.UpdateDefinition(tid, jobDefID, update)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to mark definition ready: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid":      true,
		"definition": updatedDef,
	})
}

func (h *JobHandler) DelteJob(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]

	if err := h.repo.DeleteDefinition(tid, jobDefID); err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to delete job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *JobHandler) RunJob(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]
	execution, err := h.repo.CreateExecution(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, repository.ErrJobDefinitionNotReady) {
			http.Error(w, "Job definition must be READY before execution", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create job execution: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, execution)
}

func (h *JobHandler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]
	execution, err := h.repo.GetLastExecution(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job execution not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get job execution status: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (h *JobHandler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
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

	executions, err := h.repo.ListExecutions(tid, limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, executions)
}

func (h *JobHandler) GetExecutionStats(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	days := 31 // default to 31 days
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			days = v
		}
	}

	stats, err := h.repo.ListExecutionStats(tid, days)
	if err != nil {
		http.Error(w, "Failed to get execution stats: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

func (h *JobHandler) GetJobDefinition(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	jobDefID := mux.Vars(r)["jobID"]
	definition, err := h.repo.GetJobDefinitionByID(tid, jobDefID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, definition)
}

func (h *JobHandler) GetExecution(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	execID := mux.Vars(r)["execID"]
	execution, err := h.repo.GetExecution(tid, execID)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "Job execution not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get job execution: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (h *JobHandler) SetExecutionComplete(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	execID := mux.Vars(r)["execID"]
	var req struct {
		Status           string `json:"status"`
		RecordsProcessed int64  `json:"records_processed"`
		BytesTransferred int64  `json:"bytes_transferred"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Failed to decode request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.repo.SetExecutionComplete(tid, execID, req.Status, req.RecordsProcessed, req.BytesTransferred); err != nil {
		if isNotFound(err) {
			http.Error(w, "Job execution not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to set execution complete: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *JobHandler) ListJobDefinitionsWithStats(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	stats, err := h.repo.ListJobDefinitionsWithStats(tid)
	if err != nil {
		http.Error(w, "Failed to get job definition stats: "+err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func cloneRawMessage(msg json.RawMessage) json.RawMessage {
	if len(msg) == 0 {
		return nil
	}
	return json.RawMessage(append([]byte(nil), msg...))
}

func defaultTrimmedString(value *string, fallback string) string {
	if value != nil {
		return strings.TrimSpace(*value)
	}
	return strings.TrimSpace(fallback)
}

func defaultString(value *string, fallback string) string {
	if value != nil {
		return *value
	}
	return fallback
}

func defaultRaw(value *json.RawMessage, fallback json.RawMessage) json.RawMessage {
	if value != nil {
		return cloneRawMessage(*value)
	}
	return cloneRawMessage(fallback)
}

func resolveDefinition(payload updateDefinitionPayload, current models.JobDefinition) resolvedDefinition {
	return resolvedDefinition{
		Name:                    defaultTrimmedString(payload.Name, current.Name),
		Description:             defaultString(payload.Description, current.Description),
		AST:                     defaultRaw(payload.AST, current.AST),
		SourceConnectionID:      defaultTrimmedString(payload.SourceConnectionID, current.SourceConnectionID),
		DestinationConnectionID: defaultTrimmedString(payload.DestinationConnectionID, current.DestinationConnectionID),
		ProgressSnapshot:        defaultRaw(payload.ProgressSnapshot, current.ProgressSnapshot),
	}
}

func validateResolvedDefinition(def resolvedDefinition) []string {
	var errs []string
	if strings.TrimSpace(def.Name) == "" {
		errs = append(errs, "name is required")
	}
	if len(def.AST) == 0 {
		errs = append(errs, "ast is required")
	}
	if strings.TrimSpace(def.SourceConnectionID) == "" {
		errs = append(errs, "source_connection_id is required")
	}
	if strings.TrimSpace(def.DestinationConnectionID) == "" {
		errs = append(errs, "destination_connection_id is required")
	}
	return errs
}

func decodeAllowEmpty(r *http.Request, dest interface{}) error {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
