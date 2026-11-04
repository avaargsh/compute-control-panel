package economics

import (
	"errors"
	"testing"
	"time"
)

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func goldenInputs(t *testing.T) (UsageInterval, []ProviderRateCard) {
	t.Helper()
	usage := UsageInterval{
		SchemaVersion: "compute.economics.usage-interval.v1",
		UsageIntervalID: "usage-int-001",
		TenantID: "tenant-demo",
		ProjectID: "project-demo",
		WorkloadID: "workload-demo",
		AllocationID: "allocation-demo",
		ProviderID: "provider-demo",
		PoolID: "pool-h100",
		ProfileID: "gpu-h100-80gb",
		ResourceClass: "GPU",
		GPUModel: "H100",
		Start: mustTime(t, "2026-11-01T09:00:00Z"),
		End: mustTime(t, "2026-11-01T12:00:00Z"),
		GPUCount: "2",
		Unit: "GPU_DEVICE_COUNT",
		MeteringPolicyVersion: "metering-v1",
		SourceEventIDs: []string{"allocation-boundary-start-001", "allocation-boundary-end-001"},
		Quality: "DERIVED",
		AttributionMethod: "ALLOCATION_BOUNDARY",
		SourceCoverage: 1,
	}
	rates := []ProviderRateCard{
		{
			SchemaVersion: "compute.economics.provider-rate-card.v1",
			RateCardID: "rate-h100-v1",
			Version: 1,
			ProviderID: "provider-demo",
			PoolID: "pool-h100",
			ProfileID: "gpu-h100-80gb",
			RateBasis: "ALLOCATED_GPU_HOUR",
			Unit: "GPU_HOUR",
			RateAmount: "10.00",
			Currency: "CNY",
			EffectiveFrom: mustTime(t, "2026-01-01T00:00:00Z"),
			EffectiveTo: timePtr(mustTime(t, "2026-11-01T10:30:00Z")),
			SourceRef: "contract-demo#rate-1",
			CurrencyMinorUnit: 2,
		},
		{
			SchemaVersion: "compute.economics.provider-rate-card.v1",
			RateCardID: "rate-h100-v2",
			Version: 2,
			ProviderID: "provider-demo",
			PoolID: "pool-h100",
			ProfileID: "gpu-h100-80gb",
			RateBasis: "ALLOCATED_GPU_HOUR",
			Unit: "GPU_HOUR",
			RateAmount: "12.00",
			Currency: "CNY",
			EffectiveFrom: mustTime(t, "2026-11-01T10:30:00Z"),
			SourceRef: "contract-demo#rate-2",
			CurrencyMinorUnit: 2,
		},
	}
	return usage, rates
}

func timePtr(value time.Time) *time.Time { return &value }

func TestCalculateSplitsAtEffectiveRateBoundary(t *testing.T) {
	usage, rates := goldenInputs(t)

	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.Status != StatusFinal {
		t.Fatalf("status = %q, want %q", got.Status, StatusFinal)
	}
	if got.TotalAllocatedGPUHours != "6.0" {
		t.Fatalf("total GPU-hours = %q, want 6.0", got.TotalAllocatedGPUHours)
	}
	if got.TotalDirectWorkloadCost != "66.00" || got.Currency != "CNY" {
		t.Fatalf("total cost = %s %s, want CNY 66.00", got.Currency, got.TotalDirectWorkloadCost)
	}
	if len(got.Segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(got.Segments))
	}
	want := []struct {
		start, end, quantity, amount, rateID string
		version int
	}{
		{"2026-11-01T09:00:00Z", "2026-11-01T10:30:00Z", "3.0", "30.00", "rate-h100-v1", 1},
		{"2026-11-01T10:30:00Z", "2026-11-01T12:00:00Z", "3.0", "36.00", "rate-h100-v2", 2},
	}
	for i, expected := range want {
		segment := got.Segments[i]
		if segment.Start != expected.start || segment.End != expected.end ||
			segment.PricedQuantity != expected.quantity || segment.Amount != expected.amount ||
			segment.RateCardID != expected.rateID || segment.RateCardVersion != expected.version {
			t.Errorf("segment[%d] = %+v, want %+v", i, segment, expected)
		}
		if segment.CostEntryID == "" || segment.CalculationInputDigest == "" {
			t.Errorf("segment[%d] missing deterministic identity/digest", i)
		}
	}
}

func TestRateGapReturnsIncompleteWithoutPartialCost(t *testing.T) {
	usage, rates := goldenInputs(t)
	got, err := Calculate(usage, rates[:1])
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonRateGap {
		t.Fatalf("result = %+v, want incomplete rate gap", got)
	}
	if len(got.Segments) != 0 || got.TotalDirectWorkloadCost != "" {
		t.Fatalf("incomplete calculation exposed partial posting: %+v", got)
	}
}

func TestOverlappingMatchingRatesFailClosed(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[0].EffectiveTo = nil
	_, err := Calculate(usage, rates)
	if !errors.Is(err, ErrAmbiguousRate) {
		t.Fatalf("error = %v, want ErrAmbiguousRate", err)
	}
}

func TestCurrencyChangeWithinOneIntervalFailsClosed(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[1].Currency = "USD"
	_, err := Calculate(usage, rates)
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("error = %v, want ErrCurrencyMismatch", err)
	}
}

func TestCurrencyPrecisionChangeWithinOneIntervalFailsClosed(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[1].CurrencyMinorUnit = 3
	_, err := Calculate(usage, rates)
	if !errors.Is(err, ErrCurrencyPrecisionMismatch) {
		t.Fatalf("error = %v, want ErrCurrencyPrecisionMismatch", err)
	}
}

func TestCurrencyMinorUnitControlsHalfUpRounding(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.Start = mustTime(t, "2026-11-01T09:00:00Z")
	usage.End = mustTime(t, "2026-11-01T10:00:00Z")
	usage.GPUCount = "1"
	rates = []ProviderRateCard{rates[0]}
	rates[0].EffectiveTo = nil
	rates[0].RateAmount = "1.005"
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.TotalDirectWorkloadCost != "1.01" {
		t.Fatalf("amount = %q, want 1.01 (HALF_UP)", got.TotalDirectWorkloadCost)
	}

	rates[0].RateAmount = "1.004"
	got, err = Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.TotalDirectWorkloadCost != "1.00" {
		t.Fatalf("amount = %q, want 1.00", got.TotalDirectWorkloadCost)
	}
}

func TestExactNanosecondDurationIsUsedForCurrencyAmount(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.Start = mustTime(t, "2026-11-01T09:00:00.000000000Z")
	usage.End = mustTime(t, "2026-11-01T09:00:00.000000001Z")
	usage.GPUCount = "1"
	rates = []ProviderRateCard{rates[0]}
	rates[0].EffectiveTo = nil
	rates[0].RateAmount = "3600000000000"
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.TotalDirectWorkloadCost != "1.00" {
		t.Fatalf("amount = %q, want 1.00 for one nanosecond at exact rate", got.TotalDirectWorkloadCost)
	}
}

func TestEquivalentTimestampOffsetsHaveSameDigest(t *testing.T) {
	usage, rates := goldenInputs(t)
	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("first Calculate() error = %v", err)
	}
	usage.Start = mustTime(t, "2026-11-01T17:00:00+08:00")
	usage.End = mustTime(t, "2026-11-01T20:00:00+08:00")
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("second Calculate() error = %v", err)
	}
	for i := range first.Segments {
		if first.Segments[i].CalculationInputDigest != second.Segments[i].CalculationInputDigest ||
			first.Segments[i].CostEntryID != second.Segments[i].CostEntryID {
			t.Fatalf("equivalent instants produced different identities at segment %d", i)
		}
	}
}

func TestSourceCorrectionChangesCalculationIdentity(t *testing.T) {
	usage, rates := goldenInputs(t)
	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("first Calculate() error = %v", err)
	}
	usage.SourceEventIDs = []string{"corrected-allocation-boundary-event"}
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("second Calculate() error = %v", err)
	}
	if first.Segments[0].CalculationInputDigest == second.Segments[0].CalculationInputDigest ||
		first.Segments[0].CostEntryID == second.Segments[0].CostEntryID {
		t.Fatal("corrected source facts reused the original posting identity")
	}
}

func TestIncompleteOrEstimatedUsageCannotProduceFinalCost(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.SourceCoverage = 0.95
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonIncompleteSource || len(got.Segments) != 0 {
		t.Fatalf("incomplete coverage result = %+v", got)
	}

	usage.SourceCoverage = 1
	usage.Quality = "ESTIMATED"
	got, err = Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonIncompleteSource || len(got.Segments) != 0 {
		t.Fatalf("estimated usage result = %+v", got)
	}
}

func TestInvalidWholeDeviceQuantityIsRejected(t *testing.T) {
	for _, quantity := range []string{"", "0", "1.5", "-1", "01"} {
		t.Run(quantity, func(t *testing.T) {
			usage, rates := goldenInputs(t)
			usage.GPUCount = quantity
			_, err := Calculate(usage, rates)
			if !errors.Is(err, ErrInvalidUsage) {
				t.Fatalf("error = %v, want ErrInvalidUsage", err)
			}
		})
	}
}

func TestPoolAndProfileMustMatch(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[0].ProfileID = "gpu-a100-80gb"
	rates[1].ProfileID = "gpu-a100-80gb"
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonRateGap {
		t.Fatalf("result = %+v, want rate gap", got)
	}
}

func TestUnsupportedRateBasisFailsClosed(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[0].RateBasis = "ACTIVE_GPU_HOUR"
	_, err := Calculate(usage, rates)
	if !errors.Is(err, ErrUnsupportedRateBasis) {
		t.Fatalf("error = %v, want ErrUnsupportedRateBasis", err)
	}
}

func TestCalculationIsDeterministicAcrossReplays(t *testing.T) {
	usage, rates := goldenInputs(t)
	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("first Calculate() error = %v", err)
	}
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("second Calculate() error = %v", err)
	}
	if first.TotalDirectWorkloadCost != second.TotalDirectWorkloadCost ||
		first.TotalAllocatedGPUHours != second.TotalAllocatedGPUHours ||
		len(first.Segments) != len(second.Segments) {
		t.Fatalf("replay differs: first=%+v second=%+v", first, second)
	}
	for i := range first.Segments {
		if first.Segments[i] != second.Segments[i] {
			t.Fatalf("replay segment %d differs: first=%+v second=%+v", i, first.Segments[i], second.Segments[i])
		}
	}
}
