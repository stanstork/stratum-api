package models

import "time"

// ExecutionStatDay holds counts for a single day.
type ExecutionStatDay struct {
	Day       time.Time `json:"day" db:"day"`
	Succeeded int       `json:"succeeded" db:"succeeded"`
	Failed    int       `json:"failed" db:"failed"`
	Running   int       `json:"running" db:"running"`
	Pending   int       `json:"pending" db:"pending"`
}

// ExecutionStat is the aggregated stats over a period, plus per-day details.
type ExecutionStat struct {
	Total            int                `json:"total" db:"total"`
	Succeeded        int                `json:"succeeded" db:"succeeded"`
	Failed           int                `json:"failed" db:"failed"`
	Running          int                `json:"running" db:"running"`
	SuccessRate      float64            `json:"success_rate" db:"success_rate"` // succeeded/total
	TotalDefinitions int                `json:"total_definitions" db:"total_definitions"`
	PerDay           []ExecutionStatDay `json:"per_day" db:"per_day"`
}

type JobDefinitionStat struct {
	JobDefinition

	// Calculated statistics fields
	TotalRuns             int64    `db:"total_runs" json:"total_runs"`
	LastRunStatus         *string  `db:"last_run_status" json:"last_run_status"`
	TotalBytesTransferred int64    `db:"total_bytes_transferred" json:"total_bytes_transferred"`
	AvgDurationSeconds    *float64 `db:"avg_duration_seconds" json:"avg_duration_seconds"`
}
