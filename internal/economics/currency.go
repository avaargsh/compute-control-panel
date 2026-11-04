package economics

// CurrencyMetadataVersion pins the supported subset copied from SIX Group's
// ISO 4217 List One published 2026-09-17. This is an application allowlist,
// not a claim to support every ISO 4217 code. New snapshots are additive:
// never mutate a snapshot already referenced by a persisted calculation.
//
// SIX is the official ISO 4217 Maintenance Agency.
// https://www.six-group.com/en/products-services/financial-information/market-reference-data/data-standards.html
const CurrencyMetadataVersion = "six-list-one-2026-09-17-subset-v1"

// currencyMetadataSnapshots is immutable application metadata. The nested map
// key is the exact snapshot version carried by RateCard and calculation digests.
var currencyMetadataSnapshots = map[string]map[string]int{
	CurrencyMetadataVersion: {
		"AED": 2,
		"AUD": 2,
		"BHD": 3,
		"BRL": 2,
		"CAD": 2,
		"CHF": 2,
		"CLF": 4,
		"CLP": 0,
		"CNY": 2,
		"CZK": 2,
		"DKK": 2,
		"EUR": 2,
		"GBP": 2,
		"HKD": 2,
		"INR": 2,
		"IQD": 3,
		"ISK": 0,
		"JOD": 3,
		"JPY": 0,
		"KRW": 0,
		"KWD": 3,
		"LYD": 3,
		"MXN": 2,
		"MYR": 2,
		"NOK": 2,
		"NZD": 2,
		"OMR": 3,
		"PHP": 2,
		"PLN": 2,
		"SAR": 2,
		"SEK": 2,
		"SGD": 2,
		"THB": 2,
		"TND": 3,
		"TWD": 2,
		"USD": 2,
		"VND": 0,
		"XAF": 0,
		"XOF": 0,
		"XPF": 0,
		"ZAR": 2,
	},
}
