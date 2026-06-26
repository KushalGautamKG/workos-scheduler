package worker

import "math"

const (
	defaultHighWatermarkRatio = 0.80
	defaultLowWatermarkRatio  = 0.50
)

// BackpressurePolicy decides when Kafka intake should pause or resume based on
// bounded work-queue depth and configured high/low watermark ratios.
type BackpressurePolicy struct {
	highWatermarkRatio float64
	lowWatermarkRatio  float64
}

// NewBackpressurePolicy builds a watermark policy. Invalid ratios fall back to
// defaults (high 0.80, low 0.50).
func NewBackpressurePolicy(highRatio, lowRatio float64) BackpressurePolicy {
	if !validWatermarkRatios(highRatio, lowRatio) {
		return BackpressurePolicy{
			highWatermarkRatio: defaultHighWatermarkRatio,
			lowWatermarkRatio:  defaultLowWatermarkRatio,
		}
	}

	return BackpressurePolicy{
		highWatermarkRatio: highRatio,
		lowWatermarkRatio:  lowRatio,
	}
}

func validWatermarkRatios(highRatio, lowRatio float64) bool {
	if highRatio <= 0 || highRatio > 1 {
		return false
	}
	if lowRatio < 0 || lowRatio >= 1 {
		return false
	}
	if lowRatio >= highRatio {
		return false
	}
	return true
}

// ShouldPause reports whether intake should stop at the current queue depth.
func (policy BackpressurePolicy) ShouldPause(depth, capacity int) bool {
	if capacity <= 0 {
		return false
	}

	highWatermark := int(math.Ceil(float64(capacity) * policy.highWatermarkRatio))
	return depth >= highWatermark
}

// ShouldResume reports whether intake should restart at the current queue depth.
func (policy BackpressurePolicy) ShouldResume(depth, capacity int) bool {
	if capacity <= 0 {
		return true
	}

	lowWatermark := int(math.Floor(float64(capacity) * policy.lowWatermarkRatio))
	return depth <= lowWatermark
}
