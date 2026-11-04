package economics

import (
	_ "embed"
	"encoding/json"
)

// CurrencyMetadataVersion pins the supported subset copied from SIX Group's
// ISO 4217 List One published 2026-09-17. This is an application allowlist,
// not a claim to support every ISO 4217 code. Registry snapshots are additive:
// never mutate a snapshot already referenced by a persisted calculation.
//
// SIX is the official ISO 4217 Maintenance Agency.
const CurrencyMetadataVersion = "six-list-one-2026-09-17-subset-v1"

//go:embed currency-metadata-registry-v1.json
var currencyMetadataRegistryJSON []byte

type currencyMetadataSnapshot struct {
	RegistryVersion      string         `json:"registryVersion"`
	SupportedCurrencies  map[string]int `json:"supportedCurrencies"`
}

var currencyMetadataSnapshots = loadCurrencyMetadataSnapshots()

func loadCurrencyMetadataSnapshots() map[string]map[string]int {
	var snapshot currencyMetadataSnapshot
	if err := json.Unmarshal(currencyMetadataRegistryJSON, &snapshot); err != nil {
		panic("invalid embedded currency metadata registry: " + err.Error())
	}
	if snapshot.RegistryVersion != CurrencyMetadataVersion || len(snapshot.SupportedCurrencies) == 0 {
		panic("embedded currency metadata registry version or contents are invalid")
	}
	return map[string]map[string]int{
		snapshot.RegistryVersion: snapshot.SupportedCurrencies,
	}
}
