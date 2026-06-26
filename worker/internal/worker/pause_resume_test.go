package worker

import (
	"sync"
	"testing"
)

func TestPauseResumeStartsUnpaused(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	if controller.IsPaused() {
		t.Fatal("expected controller to start unpaused")
	}
}

func TestPauseResumePauseSetsPaused(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	if err := controller.Pause(); err != nil {
		t.Fatalf("expected Pause to succeed, got %v", err)
	}
	if !controller.IsPaused() {
		t.Fatal("expected paused after Pause")
	}
}

func TestPauseResumeResumeClearsPaused(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	if err := controller.Pause(); err != nil {
		t.Fatalf("expected Pause to succeed, got %v", err)
	}
	if err := controller.Resume(); err != nil {
		t.Fatalf("expected Resume to succeed, got %v", err)
	}
	if controller.IsPaused() {
		t.Fatal("expected unpaused after Resume")
	}
}

func TestPauseResumePauseIsIdempotent(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	if err := controller.Pause(); err != nil {
		t.Fatalf("expected first Pause to succeed, got %v", err)
	}
	if err := controller.Pause(); err != nil {
		t.Fatalf("expected second Pause to be no-op, got %v", err)
	}
	if !controller.IsPaused() {
		t.Fatal("expected still paused after duplicate Pause")
	}
}

func TestPauseResumeResumeIsIdempotent(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	if err := controller.Resume(); err != nil {
		t.Fatalf("expected Resume while unpaused to be no-op, got %v", err)
	}
	if controller.IsPaused() {
		t.Fatal("expected still unpaused after duplicate Resume")
	}
}

func TestPauseResumeConcurrentCallsDoNotRaceOrPanic(t *testing.T) {
	controller := NewInMemoryPauseResumeController()

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for index := 0; index < goroutines; index++ {
		go func() {
			defer wg.Done()
			_ = controller.Pause()
		}()
	}

	wg.Wait()

	if !controller.IsPaused() {
		t.Fatal("expected paused after concurrent Pause calls")
	}

	wg.Add(goroutines)
	for index := 0; index < goroutines; index++ {
		go func() {
			defer wg.Done()
			_ = controller.Resume()
		}()
	}

	wg.Wait()

	if controller.IsPaused() {
		t.Fatal("expected unpaused after concurrent Resume calls")
	}
}
