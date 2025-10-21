package workflows

import (
	"fmt"
	"time"

	"github.com/stanstork/stratum-api/internal/temporal"
	"github.com/stanstork/stratum-api/internal/temporal/activities"
	"go.temporal.io/sdk/workflow"
)

func ExecutionWorkflow(ctx workflow.Context, params temporal.ExecutionParams) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: temporal.DefaultActivityTimeout,
		HeartbeatTimeout:    30 * time.Second, // Activities can report progress.
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting execution workflow", "TenantID", params.TenantID, "ExecutionID", params.ExecutionID)

	// Create an instance of activities struct.
	// The actual implementation is on the worker; this is just a proxy.
	var a *activities.Activities

	var preparedResult temporal.PrepareActivityResult
	defer func() {
		// If the preparation activity created a temp file, we schedule its cleanup.
		if preparedResult.ASTFilePath != "" {
			// Using a new context for cleanup to ensure it runs even if the workflow is cancelled.
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			err := workflow.ExecuteActivity(cleanupCtx, a.CleanupActivity, preparedResult.ASTFilePath).Get(cleanupCtx, nil)
			if err != nil {
				logger.Error("Failed to cleanup temporary AST file.", "path", preparedResult.ASTFilePath, "error", err)
			}
		}
	}()

	// Step 0: Create job execution record
	err := workflow.ExecuteActivity(ctx, a.CreateExecutionActivity, params.TenantID, params.JobDefinitionID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create job execution record.", "error", err)
		return err
	}

	// Step 1: Update job status to 'running'.
	err = workflow.ExecuteActivity(ctx, a.UpdateJobStatusActivity, params.TenantID, params.ExecutionID, "running", "", "").Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update job status to running.", "error", err)
		return err
	}

	// Step 2: Prepare the execution environment
	err = workflow.ExecuteActivity(ctx, a.PrepareExecutionActivity, params).Get(ctx, &preparedResult)
	if err != nil {
		msg := fmt.Sprintf("Failed to prepare execution: %v", err)
		workflow.ExecuteActivity(ctx, a.UpdateJobStatusActivity, params.TenantID, params.ExecutionID, "failed", msg, "").Get(ctx, nil)
		logger.Error("Execution preparation failed.", "error", err)
		return err
	}

	// Step 3: Run the execution container
	var containerResult temporal.RunContainerResult
	err = workflow.ExecuteActivity(ctx, a.RunExecutionContainerActivity, preparedResult).Get(ctx, &containerResult)
	if err != nil {
		msg := fmt.Sprintf("Failed to run execution container: %v", err)
		workflow.ExecuteActivity(ctx, a.UpdateJobStatusActivity, params.TenantID, params.ExecutionID, "failed", msg, "").Get(ctx, nil)
		logger.Error("Execution container execution failed.", "error", err)
		return err
	}

	// Step 4: Handle the completion logic
	err = workflow.ExecuteActivity(ctx, a.HandleCompletionActivity, containerResult).Get(ctx, nil)
	if err != nil {
		// The completion handler itself failed, which is a critical error.
		msg := fmt.Sprintf("Failed during post-execution processing: %v", err)
		workflow.ExecuteActivity(ctx, a.UpdateJobStatusActivity, params.TenantID, params.ExecutionID, "failed", msg, containerResult.Logs).Get(ctx, nil)
		logger.Error("Execution completion handling failed.", "error", err)
		return err
	}

	logger.Info("Execution workflow completed successfully.", "ExecutionID", params.ExecutionID)
	return nil
}
