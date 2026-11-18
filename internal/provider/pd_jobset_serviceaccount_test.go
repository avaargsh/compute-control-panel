package provider

import (
	"errors"
	"testing"
)

func TestPDJobSetServiceAccountBootstrapFailClosed(t *testing.T) {
	valid := pdJobSetTestConfig()
	const api = "https://kubernetes.default.svc"
	const ns = "tenant-a"
	const queue = "local-a"
	const offline = "postgres://ccp_kueue_job_binding@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1"

	for _, tc := range []struct {
		name string
		apiURL, namespace, localQueue, dsn string
		plan PDJobSetPlan
		want error
	}{
		{"no queue", api, ns, "", offline, valid, ErrInvalidRequest},
		{"invalid namespace", api, "BAD_NAMESPACE", queue, offline, valid, ErrInvalidRequest},
		{"unverified multi-GPU", api, ns, queue, offline, func() PDJobSetPlan { p := valid; p.DecodeGPUsPerReplica=2; return p }(), ErrInvalidRequest},
		{"missing durable journal", api, ns, queue, "", valid, ErrKueueBindingDSNEmpty},
		{"unreachable durable journal", api, ns, queue, offline, valid, ErrKueueBindingUnavailable},
	}{
		t.Run(tc.name,func(t *testing.T) {
			got, err := NewServiceAccountPDJobSetAdapter(
				tc.apiURL, "ca.crt", "token", tc.namespace,
				tc.localQueue, tc.dsn, tc.plan,
			)
			if got != nil || !errors.Is(err,tc.want) {
				t.Fatalf("untrusted bootstrap accepted adapter=%v err=%v want=%v", got,err,tc.want)
			}
		})
	}
}

func TestPDJobSetServiceAccountBootstrapNeverUsesMemoryBinding(t *testing.T) {
	// The test-only NewPDJobSetAdapterWithBindings can use an explicit
	// memory binding, but the production constructor MUST require a valid
	// restricted PostgreSQL binding DSN and never silently downgrade.
	got,err:=NewServiceAccountPDJobSetAdapter(
		"https://kubernetes.default.svc", "ca.crt", "token",
		"tenant-a","local-a"," ",pdJobSetTestConfig())
	if got!=nil || !errors.Is(err,ErrKueueBindingDSNEmpty) {
		t.Fatalf("production writer silently created without durable bind: %v %v",got,err)
	}
}
