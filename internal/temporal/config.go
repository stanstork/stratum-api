package temporal

import "time"

// TaskQueueName is the name of the Temporal task queue used for Stratum migration workflows.
const TaskQueueName = "STRATUM_MIGRATION"

// ExecWorkflowIDPrefix is the prefix used for Stratum migration workflow IDs.
const ExecWorkflowIDPrefix = "stratum-migration-"

// DefaultActivityTimeout is the default timeout duration for Temporal activities in Stratum migration workflows.
const DefaultActivityTimeout = 5 * time.Minute

// ExecutionParams defines the input for Stratum migration workflows.
type ExecutionParams struct {
	TenantID        string
	ExecutionID     string
	JobDefinitionID string
}

// PrepareActivityResult holds the results from the PrepareMigrationActivity.
// This data is passed to the next activity in the workflow.
type PrepareActivityResult struct {
	ASTFilePath     string
	AuthToken       string
	HostCallbackURL string
	TenantID        string
	ExecutionID     string
}

// RunContainerResult holds the results from running the Docker container.
type RunContainerResult struct {
	ExitCode    int64
	Logs        string
	TenantID    string
	ExecutionID string
}
