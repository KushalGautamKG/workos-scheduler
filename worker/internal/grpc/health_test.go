package grpc

import (
	"context"
	"testing"
)

func TestHealthStartsNotServing(t *testing.T) {
	h := NewHealth()
	status, err := h.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status != "NOT_SERVING" {
		t.Fatalf("status = %q, want NOT_SERVING", status)
	}
}

func TestHealthMarkReadyAndNotReady(t *testing.T) {
	h := NewHealth()
	h.MarkReady()
	status, err := h.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status != "SERVING" {
		t.Fatalf("status = %q, want SERVING", status)
	}

	h.MarkNotReady()
	status, err = h.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status != "NOT_SERVING" {
		t.Fatalf("status = %q, want NOT_SERVING", status)
	}
}
