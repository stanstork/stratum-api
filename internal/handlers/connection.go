package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"regexp"

	"github.com/docker/docker/client"
	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/engine"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

type testConnRequest struct {
	Format string `json:"format"`
	DSN    string `json:"dsn"`
}

type ConnectionHandler struct {
	repo          repository.ConnectionRepository
	engineClient  *engine.Client
	containerName string
}

func NewConnectionHandler(repo repository.ConnectionRepository, containerName string) *ConnectionHandler {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic("Failed to create Docker client: " + err.Error())
	}

	dr := engine.NewDockerRunner(dockerClient)
	cli := engine.NewClient(dr, containerName)
	return &ConnectionHandler{engineClient: cli, containerName: containerName, repo: repo}
}

func (h *ConnectionHandler) TestConnection(w http.ResponseWriter, r *http.Request) {
	var req testConnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	if req.Format == "" || req.DSN == "" {
		log.Println("Format and DSN are required for testing connection")
		http.Error(w, "Format and DSN are required", http.StatusBadRequest)
		return
	}

	logs, err := h.engineClient.TestConnection(r.Context(), req.Format, req.DSN)
	resp := map[string]string{"logs": ansi.ReplaceAllString(logs, "")}

	if err != nil {
		// return both the error and logs
		w.WriteHeader(http.StatusBadRequest)
		resp["error"] = ansi.ReplaceAllString(err.Error(), "")
	} else {
		w.WriteHeader(http.StatusOK)
		resp["status"] = "ok"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *ConnectionHandler) TestConnectionByID(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	id := mux.Vars(r)["id"]
	conn, err := h.repo.Get(tid, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Connection not found", http.StatusNotFound)
			return
		}
		log.Printf("Failed to get connection with ID %s: %v", id, err)
		http.Error(w, "Failed to get connection: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if conn == nil {
		log.Printf("Connection with ID %s not found", id)
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	conn_str, err := conn.GenerateConnString()
	if err != nil {
		log.Printf("Failed to generate connection string for %s: %v", id, err)
		http.Error(w, "Failed to generate connection string: "+err.Error(), http.StatusInternalServerError)
		return
	}
	logs, err := h.engineClient.TestConnection(r.Context(), conn.DataFormat, conn_str)
	resp := map[string]string{"logs": ansi.ReplaceAllString(logs, "")}

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		resp["error"] = ansi.ReplaceAllString(err.Error(), "")
	} else {
		w.WriteHeader(http.StatusOK)
		resp["status"] = "ok"
	}

	log.Printf("Tested connection %s: %s", id, resp["logs"])

	if resp["status"] == "ok" {
		conn.Status = "valid"
	} else {
		conn.Status = "invalid"
	}
	_, err = h.repo.Update(conn)
	if err != nil {
		log.Printf("Failed to update connection status for %s: %v", id, err)
		http.Error(w, "Failed to update connection status: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *ConnectionHandler) List(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	connections, err := h.repo.List(tid)
	if err != nil {
		http.Error(w, "Failed to list connections: "+err.Error(), http.StatusInternalServerError)
		return
	}

	for i := range connections {
		connections[i].Password = "" // Omit password in response for security
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(connections); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Get(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	id := mux.Vars(r)["id"]
	conn, err := h.repo.Get(tid, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Connection not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get connection: "+err.Error(), http.StatusInternalServerError)
		return
	}
	conn.Password = "" // Omit password in response for security

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(conn); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Create(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	conn.TenantID = tid

	if conn.Status == "" {
		conn.Status = "untested" // Default status if not provided
	}

	createdConn, err := h.repo.Create(&conn)
	if err != nil {
		log.Printf("Failed to create connection: %v", err)
		http.Error(w, "Failed to create connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(createdConn); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Update(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	id := mux.Vars(r)["id"]
	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	conn.ID = id // Ensure the ID is set from the URL
	conn.TenantID = tid

	updatedConn, err := h.repo.Update(&conn)
	if err != nil {
		http.Error(w, "Failed to update connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(updatedConn); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	id := mux.Vars(r)["id"]
	if err := h.repo.Delete(tid, id); err != nil {
		http.Error(w, "Failed to delete connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent) // 204 No Content
}
