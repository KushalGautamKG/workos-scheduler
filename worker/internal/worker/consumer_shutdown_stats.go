package worker

import "fmt"

// ConsumerShutdownStatsLines returns key=value lines printed when cmd/consumer stops.
func ConsumerShutdownStatsLines(stats ConsumerStats) []string {
	return []string{
		fmt.Sprintf("messages_seen=%d", stats.MessagesSeen),
		fmt.Sprintf("messages_processed=%d", stats.MessagesProcessed),
		fmt.Sprintf("message_errors=%d", stats.MessageErrors),
		fmt.Sprintf("kafka_errors=%d", stats.KafkaErrors),
		fmt.Sprintf("work_queue_capacity=%d", stats.WorkQueueCapacity),
		fmt.Sprintf("work_queue_depth=%d", stats.WorkQueueDepth),
		fmt.Sprintf("work_items_enqueued=%d", stats.WorkItemsEnqueued),
		fmt.Sprintf("work_queue_full_errors=%d", stats.WorkQueueFullErrors),
		fmt.Sprintf("backpressure_pause_events=%d", stats.BackpressurePauseEvents),
		fmt.Sprintf("backpressure_resume_events=%d", stats.BackpressureResumeEvents),
	}
}
