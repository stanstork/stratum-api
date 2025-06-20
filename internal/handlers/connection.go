package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/docker/docker/client"
	"github.com/stanstork/stratum-api/internal/engine"
)

type testConnRequest struct {
	Format string `json:"format"`
	DSN    string `json:"dsn"`
}

type ConnectionHandler struct {
	dockerClient  *client.Client
	containerName string
}

func NewConnectionHandler(containerName string) *ConnectionHandler {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic("Failed to create Docker client: " + err.Error())
	}
	return &ConnectionHandler{dockerClient: dockerClient, containerName: "stratum-engine"}
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
	resp := map[string]string{"logs": logs}

	if err != nil {
		// return both the error and logs
		w.WriteHeader(http.StatusBadRequest)
		resp["error"] = err.Error()
	} else {
		w.WriteHeader(http.StatusOK)
		resp["status"] = "ok"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
