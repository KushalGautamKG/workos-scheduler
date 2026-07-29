package metrics_test

import (
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/metrics"
)

func TestRegisterCounterRejectsDuplicate(t *testing.T) {
	metrics.ResetForTest()
	if _, err := metrics.RegisterCounter("kernelq_test_counter"); err != nil {
		t.Fatal(err)
	}
	if _, err := metrics.RegisterCounter("kernelq_test_counter"); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestRegisterResilienceMetricsIdempotent(t *testing.T) {
	metrics.ResetForTest()
	if err := metrics.RegisterResilienceMetrics(); err != nil {
		t.Fatal(err)
	}
	if err := metrics.RegisterResilienceMetrics(); err != nil {
		t.Fatal(err)
	}
	_ = metrics.IncFaultInjection("before_execute", "error")
	if metrics.FaultInjections("before_execute", "error") != 1 {
		t.Fatal("expected increment")
	}
}

func TestDuplicateDeliveryMetric(t *testing.T) {
	metrics.ResetForTest()
	_ = metrics.IncDuplicateDelivery("skipped")
	if metrics.DuplicateDeliveries("skipped") != 1 {
		t.Fatal("expected duplicate metric")
	}
}
