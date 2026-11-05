package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestE1WriterRoleAttestationRejectsDatabaseOwner(t *testing.T) {
	admin := openIntegrationDB(t)
	// The test integration DSN is PostgreSQL's migration/superuser identity;
	// it must not be usable to commit durable E1 evidence.
	if err := verifyEvidenceWriterPrivileges(context.Background(), admin); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("E1 policy table owner/superuser accepted as writer: %v", err)
	}
}
