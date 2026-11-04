package economics

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func parseTestTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func testTimePtr(value time.Time) *time.Time { return &value }

func goldenInputs(t *testing.T) (UsageInterval, []ProviderRateCard) {
	t.Helper()
	usage := UsageInterval{
		SchemaVersion: usageSchemaVersion, UsageIntervalID: "usage-int-001",
		TenantID: "tenant-demo", ProjectID: "project-demo", WorkloadID: "workload-demo",
		AllocationID: "allocation-demo", ProviderID: "provider-demo", PoolID: "pool-h100",
		ProfileID: "gpu-h100-80gb", ResourceClass: "GPU", GPUModel: "H100",
		Start:    parseTestTime(t, "2026-11-01T09:00:00Z"),
		End:      parseTestTime(t, "2026-11-01T12:00:00Z"),
		GPUCount: "2", Unit: usageUnit, MeteringPolicyVersion: "metering-v1",
		SourceEventIDs: []string{"allocation-boundary-start-001", "allocation-boundary-end-001"},
		Quality:        "DERIVED", AttributionMethod: "ALLOCATION_BOUNDARY", SourceCoverage: 1,
	}
	rates := []ProviderRateCard{
		{
			SchemaVersion: rateSchemaVersion, RateCardID: "rate-h100-v1", Version: 1,
			ProviderID: "provider-demo", PoolID: "pool-h100", ProfileID: "gpu-h100-80gb",
			RateBasis: rateBasis, Unit: rateUnit, RateAmount: "10.00", Currency: "CNY",
			EffectiveFrom: parseTestTime(t, "2026-01-01T00:00:00Z"),
			EffectiveTo:   testTimePtr(parseTestTime(t, "2026-11-01T10:30:00Z")),
			SourceRef:     "contract-demo#rate-1", CurrencyMinorUnit: 2,
			CurrencyMetadataVersion: CurrencyMetadataVersion,
		},
		{
			SchemaVersion: rateSchemaVersion, RateCardID: "rate-h100-v2", Version: 2,
			ProviderID: "provider-demo", PoolID: "pool-h100", ProfileID: "gpu-h100-80gb",
			RateBasis: rateBasis, Unit: rateUnit, RateAmount: "12.00", Currency: "CNY",
			EffectiveFrom: parseTestTime(t, "2026-11-01T10:30:00Z"),
			SourceRef:     "contract-demo#rate-2", CurrencyMinorUnit: 2,
			CurrencyMetadataVersion: CurrencyMetadataVersion,
		},
	}
	return usage, rates
}

func TestCalculateSplitsEffectiveRateBoundaryAndMatchesGoldenCost(t *testing.T) {
	usage, rates := goldenInputs(t)
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusFinal || got.TotalAllocatedGPUHours != "6.0" ||
		got.TotalDirectWorkloadCost != "66.00" || got.Currency != "CNY" ||
		got.CurrencyMetadataVersion != CurrencyMetadataVersion {
		t.Fatalf("unexpected result: %+v", got)
	}
	if len(got.Segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(got.Segments))
	}
	for i, segment := range got.Segments {
		if segment.CurrencyMetadataVersion != CurrencyMetadataVersion {
			t.Fatalf("segment %d metadata version = %q, want %q", i, segment.CurrencyMetadataVersion, CurrencyMetadataVersion)
		}
	}
	if got.Segments[0].PricedQuantity != "3.0" || got.Segments[0].Amount != "30.00" ||
		got.Segments[0].RateCardID != "rate-h100-v1" ||
		got.Segments[1].PricedQuantity != "3.0" || got.Segments[1].Amount != "36.00" ||
		got.Segments[1].RateCardID != "rate-h100-v2" {
		t.Fatalf("unexpected segments: %+v", got.Segments)
	}
	for _, segment := range got.Segments {
		if segment.CostEntryID == "" || len(segment.CalculationInputDigest) != len("sha256:")+64 {
			t.Fatalf("missing stable identity: %+v", segment)
		}
	}
}

func TestRateGapReturnsIncompleteWithoutPartialPosting(t *testing.T) {
	usage, rates := goldenInputs(t)
	got, err := Calculate(usage, rates[:1])
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonRateGap ||
		len(got.Segments) != 0 || got.TotalDirectWorkloadCost != "" {
		t.Fatalf("expected fail-closed incomplete result, got %+v", got)
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

func TestCurrencyAndPrecisionDriftFailClosed(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[1].Currency = "USD"
	if _, err := Calculate(usage, rates); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency error = %v", err)
	}
	rates[1].Currency = "CNY"
	rates[1].CurrencyMinorUnit = 3
	if _, err := Calculate(usage, rates); !errors.Is(err, ErrCurrencyPrecisionMismatch) {
		t.Fatalf("precision error = %v", err)
	}
}

func TestEmbeddedCurrencyMetadataSnapshotIsWellFormed(t *testing.T) {
	snapshot, ok := currencyMetadataSnapshots[CurrencyMetadataVersion]
	if !ok || len(snapshot) == 0 {
		t.Fatal("pinned currency metadata snapshot is missing")
	}
	for code, minorUnit := range snapshot {
		if len(code) != 3 || code != strings.ToUpper(code) {
			t.Errorf("invalid currency code %q", code)
		}
		if minorUnit < 0 || minorUnit > 4 {
			t.Errorf("invalid minor unit for %s: %d", code, minorUnit)
		}
	}
	if snapshot["CNY"] != 2 || snapshot["JPY"] != 0 || snapshot["KWD"] != 3 || snapshot["CLF"] != 4 {
		t.Fatal("pinned registry has unexpected currency precision values")
	}
}

func TestCurrencyRegistryRejectsUnknownCodesPrecisionAndVersions(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.End = parseTestTime(t, "2026-11-01T10:00:00Z")
	usage.GPUCount = "1"

	tests := []struct {
		name      string
		currency  string
		minorUnit int
		version   string
		want      error
	}{
		{name: "CNY cannot use three decimals", currency: "CNY", minorUnit: 3, version: CurrencyMetadataVersion, want: ErrCurrencyPrecisionMismatch},
		{name: "JPY cannot use two decimals", currency: "JPY", minorUnit: 2, version: CurrencyMetadataVersion, want: ErrCurrencyPrecisionMismatch},
		{name: "unknown currency is rejected", currency: "ZZZ", minorUnit: 2, version: CurrencyMetadataVersion, want: ErrUnsupportedCurrency},
		{name: "unknown metadata version is rejected", currency: "CNY", minorUnit: 2, version: "unregistered-snapshot-v9", want: ErrUnsupportedCurrencyMetadata},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rate := rates[0]
			rate.EffectiveTo = nil
			rate.Currency = test.currency
			rate.CurrencyMinorUnit = test.minorUnit
			rate.CurrencyMetadataVersion = test.version
			if _, err := Calculate(usage, []ProviderRateCard{rate}); !errors.Is(err, test.want) {
				t.Fatalf("Calculate error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDigestBindsCurrencyMetadataVersion(t *testing.T) {
	usage, rates := goldenInputs(t)
	start := usage.Start.UTC().Format(time.RFC3339Nano)
	end := parseTestTime(t, "2026-11-01T10:30:00Z").Format(time.RFC3339Nano)
	first, err := calculationDigest(usage, rates[0], start, end)
	if err != nil {
		t.Fatal(err)
	}
	changed := rates[0]
	changed.CurrencyMetadataVersion = "different-pinned-registry-snapshot"
	second, err := calculationDigest(usage, changed, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("changing currency metadata version did not change calculation digest")
	}
}

func TestCurrencyMinorUnitUsesHalfUpRounding(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.End = parseTestTime(t, "2026-11-01T10:00:00Z")
	usage.GPUCount = "1"
	rate := rates[0]
	rate.EffectiveTo = nil
	rate.RateAmount = "1.005"
	got, err := Calculate(usage, []ProviderRateCard{rate})
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalDirectWorkloadCost != "1.01" {
		t.Fatalf("HALF_UP amount = %q, want 1.01", got.TotalDirectWorkloadCost)
	}
}

func TestZeroRoundedAmountRetainsEvidenceWithoutPostingCostEntry(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.End = parseTestTime(t, "2026-11-01T09:00:01Z")
	usage.GPUCount = "1"
	rate := rates[0]
	rate.EffectiveTo = nil
	rate.RateAmount = "10.00"

	got, err := Calculate(usage, []ProviderRateCard{rate})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusFinal || got.TotalDirectWorkloadCost != "0.00" {
		t.Fatalf("unexpected fully-covered zero-minor-unit calculation: %+v", got)
	}
	if len(got.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(got.Segments))
	}
	segment := got.Segments[0]
	if segment.Amount != "0.00" || segment.PostingStatus != PostingSkippedZeroMinorUnits ||
		segment.CostEntryID != "" || segment.CalculationInputDigest == "" {
		t.Fatalf("zero-minor-unit segment must retain evidence and skip ledger posting: %+v", segment)
	}
}

func TestPositiveRoundedAmountIsMarkedPostable(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.End = parseTestTime(t, "2026-11-01T09:01:00Z")
	usage.GPUCount = "1"
	rate := rates[0]
	rate.EffectiveTo = nil
	got, err := Calculate(usage, []ProviderRateCard{rate})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Segments) != 1 || got.Segments[0].PostingStatus != PostingReady ||
		got.Segments[0].CostEntryID == "" || got.Segments[0].Amount != "0.17" {
		t.Fatalf("positive minor-unit amount must be postable: %+v", got)
	}
}

func TestNanosecondDurationIsPricedExactly(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.Start = parseTestTime(t, "2026-11-01T09:00:00.000000000Z")
	usage.End = parseTestTime(t, "2026-11-01T09:00:00.000000001Z")
	usage.GPUCount = "1"
	rate := rates[0]
	rate.EffectiveTo = nil
	rate.RateAmount = "3600000000000"
	got, err := Calculate(usage, []ProviderRateCard{rate})
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalDirectWorkloadCost != "1.00" {
		t.Fatalf("one-nanosecond cost = %q, want 1.00", got.TotalDirectWorkloadCost)
	}
}

func TestDigestIsCanonicalAcrossOffsetsAndChangesWithSourceFacts(t *testing.T) {
	usage, rates := goldenInputs(t)
	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	usage.Start = parseTestTime(t, "2026-11-01T17:00:00+08:00")
	usage.End = parseTestTime(t, "2026-11-01T20:00:00+08:00")
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	for i := range first.Segments {
		if first.Segments[i].CalculationInputDigest != second.Segments[i].CalculationInputDigest {
			t.Fatalf("equivalent instants changed digest at segment %d", i)
		}
	}
	usage.SourceEventIDs = []string{"corrected-source-event"}
	corrected, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	if second.Segments[0].CalculationInputDigest == corrected.Segments[0].CalculationInputDigest {
		t.Fatal("corrected source reused original calculation digest")
	}
}

func TestIncompleteEvidenceNeverProducesFinalCost(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.SourceCoverage = 0.95
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusIncomplete || got.Reason != ReasonIncompleteSource || len(got.Segments) != 0 {
		t.Fatalf("unexpected incomplete coverage result: %+v", got)
	}
	usage.SourceCoverage = 1
	usage.Quality = "ESTIMATED"
	got, err = Calculate(usage, rates)
	if err != nil || got.Status != StatusIncomplete || len(got.Segments) != 0 {
		t.Fatalf("estimated usage must remain incomplete: result=%+v err=%v", got, err)
	}
}

func TestInvalidWholeGpuQuantitiesAreRejected(t *testing.T) {
	for _, quantity := range []string{"", "0", "1.5", "-1", "01"} {
		t.Run(quantity, func(t *testing.T) {
			usage, rates := goldenInputs(t)
			usage.GPUCount = quantity
			if _, err := Calculate(usage, rates); !errors.Is(err, ErrInvalidUsage) {
				t.Fatalf("error = %v, want ErrInvalidUsage", err)
			}
		})
	}
}

func TestProviderProfileMismatchIsRateGapAndUnsupportedBasisFails(t *testing.T) {
	usage, rates := goldenInputs(t)
	rates[0].ProfileID = "gpu-a100-80gb"
	rates[1].ProfileID = "gpu-a100-80gb"
	got, err := Calculate(usage, rates)
	if err != nil || got.Status != StatusIncomplete || got.Reason != ReasonRateGap {
		t.Fatalf("mismatched profile result=%+v err=%v", got, err)
	}
	usage, rates = goldenInputs(t)
	rates[0].RateBasis = "ACTIVE_GPU_HOUR"
	if _, err := Calculate(usage, rates); !errors.Is(err, ErrUnsupportedRateBasis) {
		t.Fatalf("unsupported rate basis error = %v", err)
	}
}

func TestReplayProducesIdenticalCostEntries(t *testing.T) {
	usage, rates := goldenInputs(t)
	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalDirectWorkloadCost != second.TotalDirectWorkloadCost || len(first.Segments) != len(second.Segments) {
		t.Fatalf("replay result differs: first=%+v second=%+v", first, second)
	}
	for i := range first.Segments {
		if first.Segments[i] != second.Segments[i] {
			t.Fatalf("replay entry %d differs", i)
		}
	}
}


func TestDeviceBoundUsageRequiresTrustedEvidenceIdentity(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.AttributionMethod = "DEVICE_BINDING"
	if _, err := Calculate(usage, rates); !errors.Is(err, ErrInvalidUsage) {
		t.Fatalf("device-bound UsageInterval without evidence identity error = %v, want ErrInvalidUsage", err)
	}

	usage.EvidenceSetID = "evidence-set-001"
	usage.EvidenceDigest = "sha256:" + strings.Repeat("a", 64)
	usage.CapabilityProfileID = "capability-profile-h100-v1"
	got, err := Calculate(usage, rates)
	if err != nil {
		t.Fatalf("complete evidence identity rejected: %v", err)
	}
	if got.Status != StatusFinal || len(got.Segments) != 2 {
		t.Fatalf("unexpected calculation for evidence-bound UsageInterval: %+v", got)
	}
}

func TestCalculationDigestBindsEvidenceIdentity(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.AttributionMethod = "DEVICE_BINDING"
	usage.EvidenceSetID = "evidence-set-001"
	usage.EvidenceDigest = "sha256:" + strings.Repeat("a", 64)
	usage.CapabilityProfileID = "capability-profile-h100-v1"
	start := usage.Start.UTC().Format(time.RFC3339Nano)
	end := usage.End.UTC().Format(time.RFC3339Nano)

	first, err := calculationDigest(usage, rates[0], start, end)
	if err != nil {
		t.Fatal(err)
	}
	changed := usage
	changed.EvidenceDigest = "sha256:" + strings.Repeat("b", 64)
	second, err := calculationDigest(changed, rates[0], start, end)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("changing E1 evidence digest did not change cost calculation input digest")
	}
}
