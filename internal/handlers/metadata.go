package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/docker/docker/client"
	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/engine"
	"github.com/stanstork/stratum-api/internal/repository"
)

type MetadataHandler struct {
	repo          repository.ConnectionRepository
	dockerClient  *client.Client
	containerName string
}

func NewMetadataHandler(repo repository.ConnectionRepository, containerName string) *MetadataHandler {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic("Failed to create Docker client: " + err.Error())
	}
	return &MetadataHandler{repo: repo, dockerClient: dockerClient, containerName: containerName}
}

func (h *MetadataHandler) GetSourceMetadata(w http.ResponseWriter, r *http.Request) {
	tid, ok := tenantIDFromRequest(r)
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
	if conn == nil {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	dr := engine.NewDockerRunner(h.dockerClient)
	cli := engine.NewClient(dr, h.containerName)
	data, err := cli.SaveSourceMetadata(ctx, *conn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// return raw JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
