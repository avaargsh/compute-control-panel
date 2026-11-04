// Package economics provides deterministic, side-effect-free Compute Economics calculations.
package economics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	CalculationVersion     = "compute-cost-v1"
	quantityScale          = 18
	StatusFinal            = "FINAL"
	StatusIncomplete       = "INCOMPLETE"
	ReasonRateGap          = "RATE_GAP"
	ReasonIncompleteSource = "INCOMPLETE_SOURCE"
	usageSchemaVersion     = "compute.economics.usage-interval.v1"
	rateSchemaVersion      = "compute.economics.provider-rate-card.v1"
	usageUnit              = "GPU_DEVICE_COUNT"
	rateBasis              = "ALLOCATED_GPU_HOUR"
	rateUnit               = "GPU_HOUR"
)

var (
	ErrInvalidUsage              = errors.New("invalid usage interval")
	ErrInvalidRate               = errors.New("invalid provider rate card")
	ErrAmbiguousRate             = errors.New("multiple provider rates cover the same interval")
	ErrCurrencyMismatch          = errors.New("currency changes inside one usage interval")
	ErrCurrencyPrecisionMismatch = errors.New("currency precision changes inside one usage interval")
	ErrUnsupportedRateBasis      = errors.New("unsupported provider rate basis")
	ErrUnsupportedRateUnit       = errors.New("unsupported provider rate unit")
)

type UsageInterval struct {
	SchemaVersion         string
	UsageIntervalID       string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	PoolID                string
	ProfileID             string
	ResourceClass         string
	GPUModel              string
	Start                 time.Time
	End                   time.Time
	GPUCount              string
	Unit                  string
	MeteringPolicyVersion string
	SourceEventIDs        []string
	Quality               string
	AttributionMethod     string
	SourceCoverage        float64
}

type ProviderRateCard struct {
	SchemaVersion     string
	RateCardID        string
	Version           int
	ProviderID        string
	PoolID            string
	ProfileID         string
	RateBasis         string
	Unit              string
	RateAmount        string
	Currency          string
	EffectiveFrom     time.Time
	EffectiveTo       *time.Time
	SourceRef         string
	CurrencyMinorUnit int
}

type CostSegment struct {
	CostEntryID            string
	CalculationInputDigest string
	Start                  string
	End                    string
	PricedQuantity         string
	RateCardID             string
	RateCardVersion        int
	RateAmount             string
	Currency               string
	CurrencyMinorUnit      int
	Amount                 string
}

type CalculationResult struct {
	Status                  string
	Reason                  string
	UsageIntervalID         string
	TotalAllocatedGPUHours  string
	TotalDirectWorkloadCost string
	Currency                string
	Segments                []CostSegment
}

// Calculate prices a fully-covered Allocation interval against effective-dated
// Provider procurement rates. It has no persistence or billing side effects.
// Missing evidence or rate coverage returns INCOMPLETE and no partial postings.
func Calculate(usage UsageInterval, rates []ProviderRateCard) (CalculationResult, error) {
	if err := validateUsage(usage); err != nil {
		return CalculationResult{}, err
	}
	result := CalculationResult{UsageIntervalID: usage.UsageIntervalID}
	if usage.SourceCoverage != 1 || (usage.Quality != "OBSERVED" && usage.Quality != "DERIVED") {
		result.Status = StatusIncomplete
		result.Reason = ReasonIncompleteSource
		return result, nil
	}
	matching := make([]ProviderRateCard, 0, len(rates))
	for _, rate := range rates {
		if rate.ProviderID != usage.ProviderID || rate.ProfileID != usage.ProfileID {
			continue
		}
		if rate.PoolID != "" && rate.PoolID != usage.PoolID {
			continue
		}
		if err := validateRate(rate); err != nil {
			return CalculationResult{}, err
		}
		matching = append(matching, rate)
	}
	if len(matching) == 0 {
		result.Status = StatusIncomplete
		result.Reason = ReasonRateGap
		return result, nil
	}
	boundaries := []time.Time{usage.Start.UTC(), usage.End.UTC()}
	for _, rate := range matching {
		from := rate.EffectiveFrom.UTC()
		if from.After(usage.Start) && from.Before(usage.End) {
			boundaries = append(boundaries, from)
		}
		if rate.EffectiveTo != nil {
			to := rate.EffectiveTo.UTC()
			if to.After(usage.Start) && to.Before(usage.End) {
				boundaries = append(boundaries, to)
			}
		}
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Before(boundaries[j]) })
	boundaries = uniqueInstants(boundaries)
	gpuCount, ok := new(big.Int).SetString(usage.GPUCount, 10)
	if !ok || gpuCount.Sign() <= 0 {
		return CalculationResult{}, fmt.Errorf("%w: GPU count must be a positive whole number", ErrInvalidUsage)
	}
	var selectedCurrency string
	var selectedMinorUnit int
	totalQuantity := new(big.Rat)
	totalCost := new(big.Rat)
	segments := make([]CostSegment, 0, len(boundaries)-1)
	for i := 0; i < len(boundaries)-1; i++ {
		segmentStart, segmentEnd := boundaries[i], boundaries[i+1]
		var selected *ProviderRateCard
		for j := range matching {
			rate := &matching[j]
			if !rate.EffectiveFrom.After(segmentStart) &&
				(rate.EffectiveTo == nil || !segmentEnd.After(*rate.EffectiveTo)) {
				if selected != nil {
					return CalculationResult{}, ErrAmbiguousRate
				}
				selected = rate
			}
		}
		if selected == nil {
			result.Status = StatusIncomplete
			result.Reason = ReasonRateGap
			return result, nil
		}
		if selected.RateBasis != rateBasis {
			return CalculationResult{}, ErrUnsupportedRateBasis
		}
		if selected.Unit != rateUnit {
			return CalculationResult{}, ErrUnsupportedRateUnit
		}
		if selectedCurrency == "" {
			selectedCurrency = selected.Currency
			selectedMinorUnit = selected.CurrencyMinorUnit
		} else if selected.Currency != selectedCurrency {
			return CalculationResult{}, ErrCurrencyMismatch
		} else if selected.CurrencyMinorUnit != selectedMinorUnit {
			return CalculationResult{}, ErrCurrencyPrecisionMismatch
		}
		rateAmount, err := parsePositiveDecimal(selected.RateAmount)
		if err != nil {
			return CalculationResult{}, fmt.Errorf("%w: rate amount: %v", ErrInvalidRate, err)
		}
		durationNS := durationNanoseconds(segmentStart, segmentEnd)
		if durationNS.Sign() <= 0 {
			return CalculationResult{}, fmt.Errorf("%w: non-positive segment duration", ErrInvalidUsage)
		}
		// GPU-hours = whole devices × exact nanoseconds / nanoseconds per hour.
		quantity := new(big.Rat).SetFrac(
			new(big.Int).Mul(new(big.Int).Set(gpuCount), durationNS),
			big.NewInt(int64(time.Hour)),
		)
		cost := new(big.Rat).Mul(new(big.Rat).Set(quantity), rateAmount)
		roundedQuantity := roundHalfUp(quantity, quantityScale)
		roundedAmount := roundHalfUp(cost, selected.CurrencyMinorUnit)
		startText := segmentStart.UTC().Format(time.RFC3339Nano)
		endText := segmentEnd.UTC().Format(time.RFC3339Nano)
		digest, err := calculationDigest(usage, *selected, startText, endText)
		if err != nil {
			return CalculationResult{}, fmt.Errorf("calculate input digest: %w", err)
		}
		amountText := formatFixed(roundedAmount, selected.CurrencyMinorUnit)
		segment := CostSegment{
			CostEntryID:            "cost_" + strings.TrimPrefix(digest, "sha256:"),
			CalculationInputDigest: digest,
			Start:                  startText,
			End:                    endText,
			PricedQuantity:         formatTrimmed(roundedQuantity, quantityScale, 1),
			RateCardID:             selected.RateCardID,
			RateCardVersion:        selected.Version,
			RateAmount:             selected.RateAmount,
			Currency:               selected.Currency,
			CurrencyMinorUnit:      selected.CurrencyMinorUnit,
			Amount:                 amountText,
		}
		segments = append(segments, segment)
		totalQuantity.Add(totalQuantity, quantity)
		totalCost.Add(totalCost, new(big.Rat).SetFrac(roundedAmount, pow10(selected.CurrencyMinorUnit)))
	}
	result.Status = StatusFinal
	result.Currency = selectedCurrency
	result.TotalAllocatedGPUHours = formatTrimmed(roundHalfUp(totalQuantity, quantityScale), quantityScale, 1)
	result.TotalDirectWorkloadCost = formatFixed(roundHalfUp(totalCost, selectedMinorUnit), selectedMinorUnit)
	result.Segments = segments
	return result, nil
}

func validateUsage(usage UsageInterval) error {
	if usage.SchemaVersion != usageSchemaVersion ||
		!nonBlank(usage.UsageIntervalID, usage.TenantID, usage.ProjectID, usage.WorkloadID,
			usage.AllocationID, usage.ProviderID, usage.PoolID, usage.ProfileID,
			usage.MeteringPolicyVersion, usage.GPUModel) ||
		usage.ResourceClass != "GPU" || usage.Unit != usageUnit ||
		usage.Start.IsZero() || usage.End.IsZero() || !usage.End.After(usage.Start) ||
		!validWholePositiveInteger(usage.GPUCount) || usage.AllocationGeneration < 0 ||
		!validCoverage(usage.SourceCoverage) ||
		(usage.Quality != "OBSERVED" && usage.Quality != "DERIVED" &&
			usage.Quality != "ESTIMATED" && usage.Quality != "INCOMPLETE") ||
		(usage.AttributionMethod != "ALLOCATION_BOUNDARY" && usage.AttributionMethod != "DEVICE_BINDING") {
		return ErrInvalidUsage
	}
	if len(usage.SourceEventIDs) == 0 {
		return fmt.Errorf("%w: at least one source event is required", ErrInvalidUsage)
	}
	seen := make(map[string]struct{}, len(usage.SourceEventIDs))
	for _, id := range usage.SourceEventIDs {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("%w: blank source event ID", ErrInvalidUsage)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: duplicate source event ID", ErrInvalidUsage)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateRate(rate ProviderRateCard) error {
	if rate.SchemaVersion != rateSchemaVersion ||
		!nonBlank(rate.RateCardID, rate.ProviderID, rate.ProfileID, rate.RateAmount,
			rate.Currency, rate.SourceRef) ||
		rate.Version < 1 || rate.EffectiveFrom.IsZero() ||
		(rate.EffectiveTo != nil && (rate.EffectiveTo.IsZero() || !rate.EffectiveTo.After(rate.EffectiveFrom))) ||
		!validCurrency(rate.Currency) || rate.CurrencyMinorUnit < 0 || rate.CurrencyMinorUnit > 4 {
		return ErrInvalidRate
	}
	if _, err := parsePositiveDecimal(rate.RateAmount); err != nil {
		return fmt.Errorf("%w: invalid rate amount: %v", ErrInvalidRate, err)
	}
	return nil
}

func calculationDigest(usage UsageInterval, rate ProviderRateCard, start, end string) (string, error) {
	sourceIDs := append([]string(nil), usage.SourceEventIDs...)
	sort.Strings(sourceIDs)
	from := rate.EffectiveFrom.UTC().Format(time.RFC3339Nano)
	to := ""
	if rate.EffectiveTo != nil {
		to = rate.EffectiveTo.UTC().Format(time.RFC3339Nano)
	}
	canonicalRate, err := canonicalDecimal(rate.RateAmount)
	if err != nil {
		return "", err
	}
	input := struct {
		CalculationVersion    string   `json:"calculationVersion"`
		UsageSchemaVersion    string   `json:"usageSchemaVersion"`
		UsageIntervalID       string   `json:"usageIntervalId"`
		TenantID              string   `json:"tenantId"`
		ProjectID             string   `json:"projectId"`
		WorkloadID            string   `json:"workloadId"`
		AllocationID          string   `json:"allocationId"`
		AllocationGeneration  int64    `json:"allocationGeneration"`
		ProviderID            string   `json:"providerId"`
		PoolID                string   `json:"poolId"`
		ProfileID             string   `json:"profileId"`
		GPUModel              string   `json:"gpuModel"`
		GPUCount              string   `json:"gpuCount"`
		Unit                  string   `json:"unit"`
		WindowStart           string   `json:"windowStart"`
		WindowEnd             string   `json:"windowEnd"`
		MeteringPolicyVersion string   `json:"meteringPolicyVersion"`
		Quality               string   `json:"quality"`
		AttributionMethod     string   `json:"attributionMethod"`
		SourceEventIDs        []string `json:"sourceEventIds"`
		SourceCoverage        string   `json:"sourceCoverage"`
		RateCardID            string   `json:"rateCardId"`
		RateCardVersion       int      `json:"rateCardVersion"`
		RateSourceRef         string   `json:"rateSourceRef"`
		RateProviderID        string   `json:"rateProviderId"`
		RatePoolID            string   `json:"ratePoolId"`
		RateProfileID         string   `json:"rateProfileId"`
		RateBasis             string   `json:"rateBasis"`
		RateUnit              string   `json:"rateUnit"`
		RateAmount            string   `json:"rateAmount"`
		Currency              string   `json:"currency"`
		CurrencyMinorUnit     int      `json:"currencyMinorUnit"`
		EffectiveFrom         string   `json:"effectiveFrom"`
		EffectiveTo           string   `json:"effectiveTo"`
		SegmentStart          string   `json:"segmentStart"`
		SegmentEnd            string   `json:"segmentEnd"`
	}{
		CalculationVersion:    CalculationVersion,
		UsageSchemaVersion:    usage.SchemaVersion,
		UsageIntervalID:       usage.UsageIntervalID,
		TenantID:              usage.TenantID,
		ProjectID:             usage.ProjectID,
		WorkloadID:            usage.WorkloadID,
		AllocationID:          usage.AllocationID,
		AllocationGeneration:  usage.AllocationGeneration,
		ProviderID:            usage.ProviderID,
		PoolID:                usage.PoolID,
		ProfileID:             usage.ProfileID,
		GPUModel:              usage.GPUModel,
		GPUCount:              usage.GPUCount,
		Unit:                  usage.Unit,
		WindowStart:           usage.Start.UTC().Format(time.RFC3339Nano),
		WindowEnd:             usage.End.UTC().Format(time.RFC3339Nano),
		MeteringPolicyVersion: usage.MeteringPolicyVersion,
		Quality:               usage.Quality,
		AttributionMethod:     usage.AttributionMethod,
		SourceEventIDs:        sourceIDs,
		SourceCoverage:        strconv.FormatFloat(usage.SourceCoverage, 'g', -1, 64),
		RateCardID:            rate.RateCardID,
		RateCardVersion:       rate.Version,
		RateSourceRef:         rate.SourceRef,
		RateProviderID:        rate.ProviderID,
		RatePoolID:            rate.PoolID,
		RateProfileID:         rate.ProfileID,
		RateBasis:             rate.RateBasis,
		RateUnit:              rate.Unit,
		RateAmount:            canonicalRate,
		Currency:              rate.Currency,
		CurrencyMinorUnit:     rate.CurrencyMinorUnit,
		EffectiveFrom:         from,
		EffectiveTo:           to,
		SegmentStart:          start,
		SegmentEnd:            end,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func uniqueInstants(values []time.Time) []time.Time {
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if !value.Equal(unique[len(unique)-1]) {
			unique = append(unique, value)
		}
	}
	return unique
}

func durationNanoseconds(start, end time.Time) *big.Int {
	seconds := new(big.Int).Sub(big.NewInt(end.Unix()), big.NewInt(start.Unix()))
	seconds.Mul(seconds, big.NewInt(1_000_000_000))
	nanoseconds := int64(end.Nanosecond() - start.Nanosecond())
	return seconds.Add(seconds, big.NewInt(nanoseconds))
}

func parsePositiveDecimal(value string) (*big.Rat, error) {
	if value == "" || strings.TrimSpace(value) != value || strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") {
		return nil, errors.New("decimal must be an unsigned base-10 string")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" || (len(parts[0]) > 1 && parts[0][0] == '0') {
		return nil, errors.New("decimal has invalid integer part")
	}
	for _, part := range parts {
		if part == "" {
			return nil, errors.New("decimal has an empty fraction")
		}
		for _, char := range part {
			if char < '0' || char > '9' {
				return nil, errors.New("decimal contains a non-digit")
			}
		}
	}
	digits := strings.Join(parts, "")
	numerator, ok := new(big.Int).SetString(digits, 10)
	if !ok || numerator.Sign() <= 0 {
		return nil, errors.New("decimal must be positive")
	}
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}
	return new(big.Rat).SetFrac(numerator, pow10(scale)), nil
}

func canonicalDecimal(value string) (string, error) {
	if _, err := parsePositiveDecimal(value); err != nil {
		return "", err
	}
	parts := strings.Split(value, ".")
	if len(parts) == 1 {
		return parts[0], nil
	}
	fraction := strings.TrimRight(parts[1], "0")
	if fraction == "" {
		return parts[0], nil
	}
	return parts[0] + "." + fraction, nil
}

func roundHalfUp(value *big.Rat, scale int) *big.Int {
	factor := pow10(scale)
	scaledNumerator := new(big.Int).Mul(value.Num(), factor)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(scaledNumerator, value.Denom(), remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(value.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func formatFixed(value *big.Int, scale int) string {
	digits := value.String()
	if scale == 0 {
		return digits
	}
	for len(digits) <= scale {
		digits = "0" + digits
	}
	return digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
}

func formatTrimmed(value *big.Int, scale, minimumFractionDigits int) string {
	formatted := formatFixed(value, scale)
	integer, fraction, hasFraction := strings.Cut(formatted, ".")
	if !hasFraction {
		return formatted
	}
	fraction = strings.TrimRight(fraction, "0")
	for len(fraction) < minimumFractionDigits {
		fraction += "0"
	}
	if fraction == "" {
		return integer
	}
	return integer + "." + fraction
}

func pow10(scale int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
}

func validWholePositiveInteger(value string) bool {
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func validCoverage(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, char := range value {
		if char < 'A' || char > 'Z' {
			return false
		}
	}
	return true
}

func nonBlank(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}
