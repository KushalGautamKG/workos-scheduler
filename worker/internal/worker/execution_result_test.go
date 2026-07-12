package worker

import (
	"strings"
	"testing"
)

func TestSuccessResultValidates(t *testing.T) {
	result := SuccessResult()

	if err := result.Validate(); err != nil {
		t.Fatalf("expected SuccessResult to validate, got error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, result.Status)
	}
}

func TestRetryableFailureResultValidates(t *testing.T) {
	result := RetryableFailureResult("dependency timeout")

	if err := result.Validate(); err != nil {
		t.Fatalf("expected RetryableFailureResult to validate, got error: %v", err)
	}
	if result.Status != ExecutionRetryableFailure {
		t.Fatalf("expected status %q, got %q", ExecutionRetryableFailure, result.Status)
	}
	if result.Message != "dependency timeout" {
		t.Fatalf("expected message %q, got %q", "dependency timeout", result.Message)
	}
}

func TestTerminalFailureResultValidates(t *testing.T) {
	result := TerminalFailureResult("max retries exhausted")

	if err := result.Validate(); err != nil {
		t.Fatalf("expected TerminalFailureResult to validate, got error: %v", err)
	}
	if result.Status != ExecutionTerminalFailure {
		t.Fatalf("expected status %q, got %q", ExecutionTerminalFailure, result.Status)
	}
	if result.Message != "max retries exhausted" {
		t.Fatalf("expected message %q, got %q", "max retries exhausted", result.Message)
	}
}

func TestExecutionResultValidateFailsForInvalidStatus(t *testing.T) {
	result := ExecutionResult{
		Status:  ExecutionStatus("unknown"),
		Message: "something went wrong",
	}

	err := result.Validate()
	if err == nil {
		t.Fatal("expected error for invalid status, got nil")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestExecutionResultValidateAllowsBlankMessage(t *testing.T) {
	cases := []ExecutionResult{
		SuccessResult(),
		RetryableFailureResult(""),
		TerminalFailureResult(""),
	}

	for _, result := range cases {
		if err := result.Validate(); err != nil {
			t.Fatalf("expected blank message to be allowed for status %q, got error: %v", result.Status, err)
		}
	}
}

func TestExecutionStatusConstantsAreDistinct(t *testing.T) {
	statuses := []ExecutionStatus{
		ExecutionSucceeded,
		ExecutionRetryableFailure,
		ExecutionTerminalFailure,
		ExecutionDuplicateSkipped,
	}

	for i := 0; i < len(statuses); i++ {
		for j := i + 1; j < len(statuses); j++ {
			if statuses[i] == statuses[j] {
				t.Fatalf("expected distinct constants, got duplicate %q", statuses[i])
			}
		}
	}
}
