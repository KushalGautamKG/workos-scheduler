package worker

import "sync"

// PauseResumeController controls whether the Kafka poll loop should fetch new
// messages. A real implementation calls the broker; tests use in-memory fakes.
type PauseResumeController interface {
	// Pause stops intake from assigned partitions.
	Pause() error
	// Resume restarts intake after the queue has drained enough.
	Resume() error
	// IsPaused reports whether intake is currently stopped.
	IsPaused() bool
}

// InMemoryPauseResumeController is a test-friendly fake with no Kafka dependency.
// It tracks paused/unpaused state in memory so policy tests can assert transitions
// without a broker.
type InMemoryPauseResumeController struct {
	mu     sync.Mutex
	paused bool
}

// NewInMemoryPauseResumeController returns a controller that starts unpaused.
func NewInMemoryPauseResumeController() *InMemoryPauseResumeController {
	return &InMemoryPauseResumeController{}
}

// Pause marks intake as stopped. Calling Pause again while already paused is a no-op.
func (controller *InMemoryPauseResumeController) Pause() error {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	if controller.paused {
		return nil
	}

	controller.paused = true
	return nil
}

// Resume marks intake as running again. Calling Resume while already unpaused is a no-op.
func (controller *InMemoryPauseResumeController) Resume() error {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	if !controller.paused {
		return nil
	}

	controller.paused = false
	return nil
}

// IsPaused returns the current paused state.
func (controller *InMemoryPauseResumeController) IsPaused() bool {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	return controller.paused
}
