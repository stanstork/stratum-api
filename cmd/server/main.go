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
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog"
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
	logger         zerolog.Logger
	notifications  notification.Service
}

func main() {
	// Set up structured, level-based logging.
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.Kitchen}
	logger := zerolog.New(consoleWriter).With().Timestamp().Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.SetFlags(0)
	log.SetOutput(logger)

	temporalLogger := temporal.NewTemporalAdapter(logger)

	gooseAdapter := migration.NewGooseAdapter(logger)
	goose.SetLogger(gooseAdapter)

	// Load configuration.
	cfg := config.Load()

	// Initialize database connection.
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to the database")
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to ping database")
	}

	// Run database migrations.
	migration.RunMigrations(cfg.DatabaseURL, logger)

	// Initialize notification service.
	notificationRepo := repository.NewNotificationRepository(db)
	notificationService := notification.NewService(notificationRepo, logger)

	// Initialize Temporal client.
	temporalClient, err := tc.Dial(tc.Options{
		Logger: temporalLogger,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to create Temporal client")
	}
	defer temporalClient.Close()

	// Create the application instance.
	app := &application{
		config:         cfg,
		db:             db,
		temporalClient: temporalClient,
		logger:         logger,
		notifications:  notificationService,
	}

	// Start the Temporal worker in a separate goroutine.
	temporalWorker := app.startTemporalWorker(logger)

	// Initialize the HTTP router and middleware.
	router := app.initRouter(logger)
	loggedRouter := middleware.LoggingMiddleware(app.logger)(router)
	corsHandler := h.CORS(
		h.AllowedOrigins([]string{"http://localhost:3000"}),
		h.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}),
		h.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		h.AllowCredentials(),
	)(loggedRouter)

	// Start the HTTP server and handle graceful shutdown.
	app.startServer(corsHandler, temporalWorker, logger)

	logger.Info().Msg("Application terminated.")
}

// initRouter sets up all HTTP handlers and returns the router.
func (app *application) initRouter(logger zerolog.Logger) http.Handler {
	// Repositories
	jobRepo := repository.NewJobRepository(app.db)
	connRepo := repository.NewConnectionRepository(app.db)
	userRepo := repository.NewUserRepository(app.db)
	tenantRepo := repository.NewTenantRepository(app.db)
	inviteRepo := repository.NewInviteRepository(app.db)

	// Mailer for invites
	inviteMailer, err := notification.NewSMTPInviteMailer(app.config.Email)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to configure invite mailer")
	}

	// Handlers
	authHandler := handlers.NewAuthHandler(app.db, app.config, logger)
	jobHandler := handlers.NewJobHandler(jobRepo, app.temporalClient, app.notifications, logger)
	connHandler := handlers.NewConnectionHandler(connRepo, app.config.Worker.EngineImage, logger)
	metaHandler := handlers.NewMetadataHandler(connRepo, app.config.Worker.EngineImage, logger)
	reportHandler := handlers.NewReportHandler(connRepo, jobRepo, app.config.Worker.EngineImage, logger)
	tenantHandler := handlers.NewTenantHandler(tenantRepo, userRepo, logger)
	inviteHandler := handlers.NewInviteHandler(inviteRepo, tenantRepo, userRepo, inviteMailer, app.config.Email.InviteURLTemplate, logger)
	notificationHandler := handlers.NewNotificationHandler(app.notifications, logger)

	return routes.NewRouter(authHandler, jobHandler, connHandler, metaHandler, reportHandler, tenantHandler, inviteHandler, notificationHandler)
}

func (app *application) startTemporalWorker(logger zerolog.Logger) worker.Worker {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create Docker client")
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
		Notifier:          app.notifications,
	}

	w := worker.New(app.temporalClient, temporal.TaskQueueName, worker.Options{})

	w.RegisterWorkflow(workflows.ExecutionWorkflow)
	w.RegisterActivity(activityImpl)

	// Start the worker in a goroutine so it doesn't block.
	go func() {
		logger.Info().Msg("Starting Temporal worker...")
		if err := w.Run(worker.InterruptCh()); err != nil {
			logger.Fatal().Err(err).Msg("Unable to start worker")
		}
	}()

	return w
}

// startServer launches the HTTP server and handles graceful shutdown.
func (app *application) startServer(handler http.Handler, temporalWorker worker.Worker, logger zerolog.Logger) {
	server := &http.Server{
		Addr:    ":" + app.config.ServerPort,
		Handler: handler,
	}

	// Channel to listen for server errors
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info().Msgf("Server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// Wait for an interrupt signal or a server error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info().Msgf("Received signal: %s. Shutting down...", sig)
	case err := <-serverErrCh:
		logger.Error().Err(err).Msg("Server error occurred")
	}

	// Gracefully shut down the HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("HTTP server shutdown error")
	} else {
		logger.Info().Msg("HTTP server shutdown complete.")
	}

	// Stop the Temporal worker.
	logger.Info().Msg("Stopping Temporal worker...")
	temporalWorker.Stop()
	logger.Info().Msg("Temporal worker stopped.")
}
