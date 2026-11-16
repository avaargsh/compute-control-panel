package provider

import (
 "crypto/ed25519"
 "crypto/rand"
 "testing"
 "time"
)

// A trusted registry may rotate or reuse its input buffers. A verifier must
// retain its own pinned key and must not follow caller-side mutations.
func TestDRAUnprepareVerifierPinsIndependentKey(t *testing.T) {
 publicKey, _, err := ed25519.GenerateKey(rand.Reader)
 if err != nil { t.Fatal(err) }
 original := append(ed25519.PublicKey(nil), publicKey...)
 verifier, err := NewDRAUnprepareVerifier(DRAUnprepareAuthority{
  ClusterID: "cluster-a", SourceID: "driver-source-a", KeyID: "key-a",
  Driver: "gpu.example.com", PublicKey: publicKey,
 }, time.Minute, time.Second)
 if err != nil { t.Fatalf("construct verifier: %v", err) }
 publicKey[0] ^= 0xff
 if string(verifier.authority.PublicKey) != string(original) {
  t.Fatal("verifier's pinned key changed with caller-owned buffer")
 }
}

// The signed driver receipt has no authority without a separately provisioned
// key and bounded freshness policy. Invalid trust configuration must fail closed.
func TestDRAUnprepareVerifierRejectsInvalidAuthorityAndClock(t *testing.T) {
 publicKey, _, err := ed25519.GenerateKey(rand.Reader)
 if err != nil { t.Fatal(err) }
 valid := DRAUnprepareAuthority{
  ClusterID: "cluster-a", SourceID: "driver-source-a", KeyID: "key-a",
  Driver: "gpu.example.com", PublicKey: publicKey,
 }
 cases := []struct {
  name string
  mutate func(*DRAUnprepareAuthority)
  age time.Duration
  skew time.Duration
 }{
  {"missing cluster", func(a *DRAUnprepareAuthority) { a.ClusterID = "" }, time.Minute, 0},
  {"missing source", func(a *DRAUnprepareAuthority) { a.SourceID = "" }, time.Minute, 0},
  {"missing key identity", func(a *DRAUnprepareAuthority) { a.KeyID = "" }, time.Minute, 0},
  {"missing driver", func(a *DRAUnprepareAuthority) { a.Driver = "" }, time.Minute, 0},
  {"short key", func(a *DRAUnprepareAuthority) { a.PublicKey = a.PublicKey[:31] }, time.Minute, 0},
  {"zero age", func(*DRAUnprepareAuthority) {}, 0, 0},
  {"negative skew", func(*DRAUnprepareAuthority) {}, time.Minute, -time.Second},
  {"skew exceeds age", func(*DRAUnprepareAuthority) {}, time.Second, time.Minute},
 }
 for _, tc := range cases {
  t.Run(tc.name, func(t *testing.T) {
   authority := valid
   tc.mutate(&authority)
   if _, err := NewDRAUnprepareVerifier(authority, tc.age, tc.skew); err == nil {
    t.Fatal("invalid source authority or freshness policy was accepted")
   }
  })
 }
}
