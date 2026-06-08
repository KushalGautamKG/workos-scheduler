// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the result publishing boundary. After a worker executes
// a job, it can publish a WorkerResultEvent so the control plane learns the
// outcome. Real Kafka publishing comes later; tests use RecordingResultProducer.
package worker

// ResultProducer publishes validated worker result events.
//
// Production code will use a Kafka-backed implementation (kernelq.jobs.results).
// Tests use RecordingResultProducer to capture events in memory—no broker needed.
type ResultProducer interface {
	PublishResult(event WorkerResultEvent) error
}

// RecordingResultProducer is an in-memory ResultProducer for tests and demos.
//
// It validates each event and appends it to Published. Nothing is sent to Kafka.
type RecordingResultProducer struct {
	Published []WorkerResultEvent
}

// PublishResult validates the event and stores a copy in Published.
func (p *RecordingResultProducer) PublishResult(event WorkerResultEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}

	p.Published = append(p.Published, event)
	return nil
}
