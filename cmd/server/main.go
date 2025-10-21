package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/docker/docker/client"
	h "github.com/gorilla/handlers"
	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/handlers"
	"github.com/stanstork/stratum-api/internal/middleware"
	"github.com/stanstork/stratum-api/internal/migration"
	"github.com/stanstork/stratum-api/internal/notification"
	"github.com/stanstork/stratum-api/internal/repository"
	"github.com/stanstork/stratum-api/internal/routes"
	"github.com/stanstork/stratum-api/internal/temporal"
	"github.com/stanstork/stratum-api/internal/temporal/activities"
	"github.com/stanstork/stratum-api/internal/temporal/workflows"

	_ "github.com/lib/pq" // PostgreSQL driver
	tc "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type application struct {
	config         *config.Config
	db             *sql.DB
	temporalClient tc.Client
}

func main() {
	// Load configuration.
	cfg := config.Load()

	// Initialize database connection.
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Run database migrations.
	migration.RunMigrations(cfg.DatabaseURL)

	// Initialize Temporal client.
	temporalClient, err := tc.Dial(tc.Options{})
	if err != nil {
		log.Fatalf("Unable to create Temporal client: %v", err)
	}
	defer temporalClient.Close()

	// Create the application instance.
	app := &application{
		config:         cfg,
		db:             db,
		temporalClient: temporalClient,
	}

	// Start the Temporal worker in a separate goroutine.
	temporalWorker := app.startTemporalWorker()

	// Initialize the HTTP router and middleware.
	router := app.initRouter()
	loggedRouter := middleware.LoggingMiddleware(router)
	corsHandler := h.CORS(
		h.AllowedOrigins([]string{"http://localhost:3000"}),
		h.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}),
		h.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		h.AllowCredentials(),
	)(loggedRouter)

	// Start the HTTP server and handle graceful shutdown.
	app.startServer(corsHandler, temporalWorker)

	log.Println("Application terminated.")
}

// initRouter sets up all HTTP handlers and returns the router.
func (app *application) initRouter() http.Handler {
	// Repositories
	jobRepo := repository.NewJobRepository(app.db)
	connRepo := repository.NewConnectionRepository(app.db)
	userRepo := repository.NewUserRepository(app.db)
	tenantRepo := repository.NewTenantRepository(app.db)
	inviteRepo := repository.NewInviteRepository(app.db)

	// Mailer for invites
	inviteMailer, err := notification.NewSMTPInviteMailer(app.config.Email)
	if err != nil {
		log.Fatalf("failed to configure invite mailer: %v", err)
	}

	// Handlers
	authHandler := handlers.NewAuthHandler(app.db, app.config)
	jobHandler := handlers.NewJobHandler(jobRepo, app.temporalClient)
	connHandler := handlers.NewConnectionHandler(connRepo, app.config.Worker.EngineImage)
	metaHandler := handlers.NewMetadataHandler(connRepo, app.config.Worker.EngineImage)
	reportHandler := handlers.NewReportHandler(connRepo, jobRepo, app.config.Worker.EngineImage)
	tenantHandler := handlers.NewTenantHandler(tenantRepo, userRepo)
	inviteHandler := handlers.NewInviteHandler(inviteRepo, tenantRepo, userRepo, inviteMailer, app.config.Email.InviteURLTemplate)

	return routes.NewRouter(authHandler, jobHandler, connHandler, metaHandler, reportHandler, tenantHandler, inviteHandler)
}

func (app *application) startTemporalWorker() worker.Worker {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to create Docker client: %v", err)
	}

	activityImpl := &activities.Activities{
		JobRepo:           repository.NewJobRepository(app.db),
		ConnRepo:          repository.NewConnectionRepository(app.db),
		DockerClient:      dockerClient,
		EngineImage:       app.config.Worker.EngineImage,
		JWTSigningKey:     []byte(app.config.JWTSecret),
		TempDir:           app.config.Worker.TempDir,
		ContainerCPULimit: app.config.Worker.ContainerCPULimit,
		ContainerMemLimit: app.config.Worker.ContainerMemoryLimit,
	}

	w := worker.New(app.temporalClient, temporal.TaskQueueName, worker.Options{})

	w.RegisterWorkflow(workflows.ExecutionWorkflow)
	w.RegisterActivity(activityImpl)

	// Start the worker in a goroutine so it doesn't block.
	go func() {
		log.Println("Starting Temporal worker...")
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Fatalf("Unable to start worker: %v", err)
		}
	}()

	return w
}

// startServer launches the HTTP server and handles graceful shutdown.
func (app *application) startServer(handler http.Handler, temporalWorker worker.Worker) {
	server := &http.Server{
		Addr:    ":" + app.config.ServerPort,
		Handler: handler,
	}

	// Channel to listen for server errors
	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("Server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// Wait for an interrupt signal or a server error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("Received signal: %s. Shutting down...", sig)
	case err := <-serverErrCh:
		log.Printf("Server error: %v. Shutting down...", err)
	}

	// Gracefully shut down the HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	} else {
		log.Println("HTTP server shutdown complete.")
	}

	// Stop the Temporal worker.
	log.Println("Stopping Temporal worker...")
	temporalWorker.Stop()
	log.Println("Temporal worker stopped.")
}
