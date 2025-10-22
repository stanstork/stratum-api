package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/docker/docker/client"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/engine"
	"github.com/stanstork/stratum-api/internal/repository"
)

var dataFormatMap = map[string]string{
	"pg":         "Postgres",
	"postgresql": "Postgres",
	"postgres":   "Postgres",
	"mysql":      "MySql",
}

type ReportHandler struct {
	conn         repository.ConnectionRepository
	job          repository.JobRepository
	engineClient *engine.Client
	logger       zerolog.Logger
}

func NewReportHandler(conn repository.ConnectionRepository, job repository.JobRepository, containerName string, logger zerolog.Logger) *ReportHandler {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create Docker client")
	}

	dr := engine.NewDockerRunner(dockerClient)
	engineClient := engine.NewClient(dr, containerName)
	return &ReportHandler{conn: conn, job: job, engineClient: engineClient, logger: logger}
}

func (h *ReportHandler) DryRunReport(w http.ResponseWriter, r *http.Request) {
	tid, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}
	defID := mux.Vars(r)["definition_id"]

	// Load definition
	def, err := h.job.GetJobDefinitionByID(tid, defID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Job definition not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get job definition: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Load connections
	srcConn, err := h.conn.Get(tid, def.SourceConnectionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Source connection not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get source connection: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if srcConn == nil {
		http.Error(w, "Source connection not found", http.StatusBadRequest)
		return
	}

	destConn, err := h.conn.Get(tid, def.DestinationConnectionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Destination connection not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get destination connection: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if destConn == nil {
		http.Error(w, "Destination connection not found", http.StatusBadRequest)
		return
	}

	// Parse AST as generic map so we can inject connections
	var ast map[string]interface{}
	if err := json.Unmarshal(def.AST, &ast); err != nil {
		http.Error(w, "Failed to parse AST: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ast == nil {
		http.Error(w, "AST is empty or invalid", http.StatusBadRequest)
		return
	}

	// Build connection strings
	srcConnStr, err := srcConn.GenerateConnString()
	if err != nil {
		http.Error(w, "Failed to generate source connection string: "+err.Error(), http.StatusInternalServerError)
		return
	}
	destConnStr, err := destConn.GenerateConnString()
	if err != nil {
		http.Error(w, "Failed to generate destination connection string: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ast["connections"] = map[string]interface{}{
		"source": map[string]interface{}{
			"conn_type": "Source",
			"format":    dataFormatMap[def.SourceConnection.DataFormat],
			"conn_str":  srcConnStr,
		},
		"dest": map[string]interface{}{
			"conn_type": "Dest",
			"format":    dataFormatMap[def.DestinationConnection.DataFormat],
			"conn_str":  destConnStr,
		},
	}

	cfgBytes, err := json.Marshal(ast)
	if err != nil {
		http.Error(w, "Failed to serialize AST: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Timeout & output path from query
	timeout := 30 * time.Second
	if ts := r.URL.Query().Get("timeout_s"); ts != "" {
		if v, perr := strconv.Atoi(ts); perr == nil && v > 0 {
			timeout = time.Duration(v) * time.Second
		}
	}

	// Set a timeout for the dry run operation
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	report, err := h.engineClient.DryRun(ctx, cfgBytes)
	if err != nil {
		// map timeouts to 504; other engine failures to 502
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "dry-run timed out", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "dry-run failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	// Return JSON bytes produced by engine
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if r.URL.Query().Get("download") == "1" {
		w.Header().Set("Content-Disposition", `attachment; filename="dryrun_report.json"`)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(report)
}
