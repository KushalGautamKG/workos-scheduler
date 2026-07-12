package worker

import (
	"fmt"
	"strings"
)

// ExecutionIdempotencyKey builds the logical claim key for worker execution
// dedupe. Format matches Python execution_key(job_id, attempt):
//
//	execution:<job_id>:<attempt>
func ExecutionIdempotencyKey(jobID string, attempt int) (string, error) {
	if strings.TrimSpace(jobID) == "" {
		return "", fmt.Errorf("job_id must be a non-empty string")
	}
	if attempt < 0 {
		return "", fmt.Errorf("attempt must be >= 0, got %d", attempt)
	}
	return fmt.Sprintf("execution:%s:%d", strings.TrimSpace(jobID), attempt), nil
}
