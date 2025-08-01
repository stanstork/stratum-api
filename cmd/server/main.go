package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	h "github.com/gorilla/handlers"
	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/handlers"
	"github.com/stanstork/stratum-api/internal/middleware"
	"github.com/stanstork/stratum-api/internal/migration"
	"github.com/stanstork/stratum-api/internal/repository"
	"github.com/stanstork/stratum-api/internal/routes"
	"github.com/stanstork/stratum-api/internal/worker"
)

func main() {
	// load configuration
	cfg := loadConfig()

	// initialize database
	db := initDatabase(cfg)
	defer db.Close()

	// applies any pending database migrations.
	migration.RunMigrations(cfg.DatabaseURL)

	// initialize HTTP handlers and router
	router := initRouter(db, cfg)

	// set up logging middleware
	router = middleware.LoggingMiddleware(router)

	// set up CORS options
	corsOpts := []h.CORSOption{
		h.AllowedOrigins([]string{"http://localhost:3000"}),
		h.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}),
		h.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		h.AllowCredentials(),
	}

	// wrap router with the CORS middleware
	corsHandler := h.CORS(corsOpts...)(router)

	// initialize and start the migration worker
	_, workerCancel := initWorker(db, cfg.Worker)
	defer workerCancel()

	// start HTTP server and handle graceful shutdown
	startServer(corsHandler, cfg.ServerPort, workerCancel)

	log.Println("Application terminated.")
}

// loadConfig loads and returns the application configuration.
func loadConfig() *config.Config {
	return config.Load()
}

// initDatabase opens a PostgreSQL connection and returns it.
func initDatabase(cfg *config.Config) *sql.DB {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// initRouter sets up authentication, job handlers, and returns the HTTP router.
func initRouter(db *sql.DB, cfg *config.Config) http.Handler {
	jobRepo := repository.NewJobRepository(db)
	connRepo := repository.NewConnectionRepository(db)

	authHandler := handlers.NewAuthHandler(db, cfg)
	jobHandler := handlers.NewJobHandler(jobRepo)
	connHandler := handlers.NewConnectionHandler(connRepo, cfg.Worker.EngineContainer)
	metaHandler := handlers.NewMetadataHandler(connRepo, cfg.Worker.EngineContainer)

	return routes.NewRouter(authHandler, jobHandler, connHandler, metaHandler)
}

// initWorker constructs, starts, and returns the worker’s context cancel function.
func initWorker(db *sql.DB, cfg config.WorkerConfig) (context.Context, context.CancelFunc) {
	jobRepo := repository.NewJobRepository(db)
	connRepo := repository.NewConnectionRepository(db)
	workerCfg := &worker.WorkerConfig{
		DB:                   db,
		JobRepo:              jobRepo,
		ConnRepo:             connRepo,
		PollInterval:         cfg.PollInterval,
		EngineImage:          cfg.EngineImage,
		TempDir:              cfg.TempDir,
		ContainerCPULimit:    cfg.ContainerCPULimit,
		ContainerMemoryLimit: cfg.ContainerMemoryLimit,
	}

	w, err := worker.NewWorker(*workerCfg)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := w.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Worker stopped with error: %v", err)
		}
	}()

	return ctx, cancel
}

// startServer launches the HTTP server and waits for OS signals.
// When a shutdown signal arrives, it gracefully stops the server and calls workerCancel.
func startServer(handler http.Handler, port string, workerCancel context.CancelFunc) {
	addr := ":" + port
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	// Channel to listen for server errors
	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("Server listening on %s", addr)
		serverErrCh <- server.ListenAndServe()
	}()

	// Channel to listen for OS interrupt or termination signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Wait for either a server error or a shutdown signal
	select {
	case sig := <-stop:
		log.Printf("Received signal: %v. initiating shutdown...", sig)
	case err := <-serverErrCh:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}

	// Begin graceful shutdown with a timeout context
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Http server shutdown error: %v", err)
	} else {
		log.Println("Http server shutdown complete")
	}

	// Stop the worker’s polling loop
	workerCancel()

	// Give the worker a moment to finish any ongoing job
	time.Sleep(2 * time.Second)
	log.Println("Worker shutdown complete")
}
