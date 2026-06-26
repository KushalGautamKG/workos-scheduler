package worker

import "testing"

func defaultBackpressurePolicy() BackpressurePolicy {
	return NewBackpressurePolicy(0.80, 0.50)
}

func TestDefaultBackpressurePolicyPausesAtHighWatermark(t *testing.T) {
	policy := defaultBackpressurePolicy()

	if !policy.ShouldPause(80, 100) {
		t.Fatal("expected default policy to pause at depth 80 with capacity 100")
	}
}

func TestDefaultBackpressurePolicyDoesNotPauseBelowHighWatermark(t *testing.T) {
	policy := defaultBackpressurePolicy()

	if policy.ShouldPause(79, 100) {
		t.Fatal("expected default policy not to pause below high watermark at depth 79")
	}
}

func TestDefaultBackpressurePolicyResumesAtLowWatermark(t *testing.T) {
	policy := defaultBackpressurePolicy()

	if !policy.ShouldResume(50, 100) {
		t.Fatal("expected default policy to resume at depth 50 with capacity 100")
	}
}

func TestDefaultBackpressurePolicyDoesNotResumeAboveLowWatermark(t *testing.T) {
	policy := defaultBackpressurePolicy()

	if policy.ShouldResume(51, 100) {
		t.Fatal("expected default policy not to resume above low watermark at depth 51")
	}
}

func TestCustomBackpressureWatermarks(t *testing.T) {
	policy := NewBackpressurePolicy(0.60, 0.30)

	if policy.ShouldPause(59, 100) {
		t.Fatal("expected no pause below custom high watermark at depth 59")
	}
	if !policy.ShouldPause(60, 100) {
		t.Fatal("expected pause at custom high watermark ceil(100*0.60)=60")
	}
	if policy.ShouldResume(31, 100) {
		t.Fatal("expected no resume above custom low watermark at depth 31")
	}
	if !policy.ShouldResume(30, 100) {
		t.Fatal("expected resume at custom low watermark floor(100*0.30)=30")
	}
}

func TestInvalidBackpressureWatermarksFallBackToDefaults(t *testing.T) {
	cases := []struct {
		name string
		high float64
		low  float64
	}{
		{name: "high zero", high: 0, low: 0.50},
		{name: "high above one", high: 1.1, low: 0.50},
		{name: "low negative", high: 0.80, low: -0.1},
		{name: "low at least one", high: 0.80, low: 1.0},
		{name: "low not below high", high: 0.80, low: 0.80},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := NewBackpressurePolicy(tc.high, tc.low)

			if !policy.ShouldPause(80, 100) {
				t.Fatal("expected default high watermark pause at depth 80")
			}
			if policy.ShouldPause(79, 100) {
				t.Fatal("expected default high watermark not to pause below depth 80")
			}
			if !policy.ShouldResume(50, 100) {
				t.Fatal("expected default low watermark resume at depth 50")
			}
			if policy.ShouldResume(51, 100) {
				t.Fatal("expected default low watermark not to resume above depth 50")
			}
		})
	}
}

func TestBackpressurePolicyNonPositiveCapacityNeverPausesAndAlwaysResumes(t *testing.T) {
	policy := defaultBackpressurePolicy()

	for _, capacity := range []int{0, -1} {
		if policy.ShouldPause(10, capacity) {
			t.Fatalf("expected ShouldPause false for capacity %d", capacity)
		}
		if !policy.ShouldResume(10, capacity) {
			t.Fatalf("expected ShouldResume true for capacity %d", capacity)
		}
	}
}

func TestBackpressurePolicyCeilAndFloorForOddCapacities(t *testing.T) {
	policy := defaultBackpressurePolicy()

	// capacity 7: ceil(7*0.80)=6, floor(7*0.50)=3
	if policy.ShouldPause(5, 7) {
		t.Fatal("expected no pause below ceil(7*0.80)=6 at depth 5")
	}
	if !policy.ShouldPause(6, 7) {
		t.Fatal("expected pause at ceil(7*0.80)=6")
	}
	if !policy.ShouldResume(3, 7) {
		t.Fatal("expected resume at floor(7*0.50)=3")
	}
	if policy.ShouldResume(4, 7) {
		t.Fatal("expected no resume above floor(7*0.50)=3 at depth 4")
	}

	// capacity 3: ceil(3*0.80)=3, floor(3*0.50)=1
	if policy.ShouldPause(2, 3) {
		t.Fatal("expected no pause below ceil(3*0.80)=3 at depth 2")
	}
	if !policy.ShouldPause(3, 3) {
		t.Fatal("expected pause at ceil(3*0.80)=3")
	}
	if !policy.ShouldResume(1, 3) {
		t.Fatal("expected resume at floor(3*0.50)=1")
	}
	if policy.ShouldResume(2, 3) {
		t.Fatal("expected no resume above floor(3*0.50)=1 at depth 2")
	}
}
