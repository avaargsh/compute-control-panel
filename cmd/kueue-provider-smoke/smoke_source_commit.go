package main

import "errors"

// A Git checkout revision is useful CI reproducibility metadata only. It is
// NOT a signature, a driver identity or an authenticated evidence source.
// Fail closed rather than publish M1/M2 with an absent or malformed commit.
var errInvalidSmokeSourceCommit = errors.New("disposable evidence requires exact lowercase 40-hex checkout revision")

func checkedSmokeSourceCommit(getenv func(string) string) (string, error) {
	if getenv == nil {
		return "", errInvalidSmokeSourceCommit
	}
	commit := getenv("CCP_SMOKE_SOURCE_COMMIT")
	if len(commit) != 40 {
		return "", errInvalidSmokeSourceCommit
	}
	for i := 0; i < len(commit); i++ {
		ch := commit[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return "", errInvalidSmokeSourceCommit
		}
	}
	return commit, nil
}
