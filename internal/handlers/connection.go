package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"

	"github.com/docker/docker/client"
	"github.com/gorilla/mux"
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
	dockerClient  *client.Client
	containerName string
}

func NewConnectionHandler(repo repository.ConnectionRepository, containerName string) *ConnectionHandler {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic("Failed to create Docker client: " + err.Error())
	}
	return &ConnectionHandler{dockerClient: dockerClient, containerName: containerName, repo: repo}
}

func (h *ConnectionHandler) TestConnection(w http.ResponseWriter, r *http.Request) {
	var req testConnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	if req.Format == "" || req.DSN == "" {
		http.Error(w, "Format and DSN are required", http.StatusBadRequest)
		return
	}

	logs, err := engine.TestConnectionByExec(r.Context(), h.dockerClient, h.containerName, req.Format, req.DSN)
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

func (h *ConnectionHandler) List(w http.ResponseWriter, r *http.Request) {
	connections, err := h.repo.List()
	if err != nil {
		http.Error(w, "Failed to list connections: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(connections); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	conn, err := h.repo.Get(id)
	if err != nil {
		http.Error(w, "Failed to get connection: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if conn == nil {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(conn); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (h *ConnectionHandler) Create(w http.ResponseWriter, r *http.Request) {
	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

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
	id := mux.Vars(r)["id"]
	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	conn.ID = id // Ensure the ID is set from the URL

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
	id := mux.Vars(r)["id"]
	if err := h.repo.Delete(id); err != nil {
		http.Error(w, "Failed to delete connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent) // 204 No Content
}
