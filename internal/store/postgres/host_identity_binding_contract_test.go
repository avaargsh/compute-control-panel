package postgres

import (
	"context"
	"testing"
	"time"
)

// This is the first durable enrollment contract, not an authorization or
// physical GPU attestation test. Only the disposable integration DB is used.
// Runtime enrollment/heartbeat writers must remain unmounted until their
// separate least-privilege, attestation and session CAS gates pass.
func TestHostIdentityBindingDatabaseContract(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("provider fixture: created=%v err=%v", created, err)
	}
	hostID := "host-" + randomSuffix(t)
	nodeUID := "node-" + randomSuffix(t)
	subject := "spiffe://ccp.example/host/" + hostID

	const insert = `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject)
		VALUES($1,$2,$3,$4,$5)`
	if _, err := db.ExecContext(ctx, insert, hostID, p.ProviderID, p.ClusterScope, nodeUID, subject); err != nil {
		t.Fatalf("valid immutable binding must persist: %v", err)
	}

	for _, tc := range []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "duplicate native node identity",
			query: insert,
			args:  []any{hostID + "-duplicate", p.ProviderID, p.ClusterScope, nodeUID, subject},
		},
		{
			name:  "unknown provider",
			query: insert,
			args:  []any{hostID + "-unknown", p.ProviderID + "-absent", p.ClusterScope, nodeUID + "-new", subject},
		},
		{
			name:  "cluster mismatch",
			query: insert,
			args:  []any{hostID + "-cluster", p.ProviderID, p.ClusterScope + "-foreign", nodeUID + "-new", subject},
		},
		{
			name:  "missing native UID",
			query: insert,
			args:  []any{hostID + "-no-uid", p.ProviderID, p.ClusterScope, "", subject},
		},
		{
			name:  "missing subject",
			query: insert,
			args:  []any{hostID + "-no-subject", p.ProviderID, p.ClusterScope, nodeUID + "-new", ""},
		},
		{
			name:  "successor Node adoption",
			query: "UPDATE public.compute_host_identity_bindings SET node_uid=$2 WHERE host_id=$1",
			args:  []any{hostID, nodeUID + "-successor"},
		},
		{
			name:  "principal reassignment",
			query: "UPDATE public.compute_host_identity_bindings SET subject=$2 WHERE host_id=$1",
			args:  []any{hostID, subject + "-successor"},
		},
		{
			name:  "deletion of enrolled identity",
			query: "DELETE FROM public.compute_host_identity_bindings WHERE host_id=$1",
			args:  []any{hostID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, tc.query, tc.args...); err == nil {
				t.Fatalf("unsafe enrollment mutation succeeded: %s", tc.name)
			}
		})
	}

	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "PLANNED_MAINTENANCE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, insert, hostID+"-after-drain", p.ProviderID, p.ClusterScope, nodeUID+"-new", subject); err == nil {
		t.Fatal("draining Provider admitted a new host binding")
	}
	var provider, cluster, uid, persistedSubject string
	if err := db.QueryRowContext(ctx,
		"SELECT provider_id, cluster_scope, node_uid, subject FROM public.compute_host_identity_bindings WHERE host_id=$1",
		hostID).Scan(&provider, &cluster, &uid, &persistedSubject); err != nil {
		t.Fatal(err)
	}
	if provider != p.ProviderID || cluster != p.ClusterScope || uid != nodeUID || persistedSubject != subject {
		t.Fatal("rejected writes changed immutable enrollment")
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_host_identity_bindings WHERE provider_id=$1",
		p.ProviderID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("provider host cardinality=%d err=%v, want one", count, err)
	}
}
