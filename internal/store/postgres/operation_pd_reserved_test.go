package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// Existing generic worker dispatch must not claim a reserved PD operation.
// This check is required even before the isolated PD staging schema is
// promoted: a PD worker rechecks the independent authority in a separate
// same-transaction path, not through generic Claim or Start.
func reservedPDOperationFixture(t *testing.T) (*Repository, context.Context, context.CancelFunc, operation.Operation) {
	t.Helper()
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	repo, err := NewRepository(db)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	tenant := "tenant-pd-reserved-" + randomSuffix(t)
	op := operationFixture(t, repo, ctx, tenant)
	hash := sha256.Sum256([]byte(randomSuffix(t)))
	op.ID = fmt.Sprintf("pd-operation-%x", hash)
	if _, created, err := repo.CreateOperation(ctx, op,
		"event-pd-reserved-"+randomSuffix(t), "corr-pd-reserved"); err != nil || !created {
		cancel()
		t.Fatalf("persist reserved PD fixture: created=%t err=%v", created, err)
	}
	return repo, ctx, cancel, op
}

func TestGenericClaimRejectsReservedPDOperation(t *testing.T) {
	repo, ctx, cancel, reserved := reservedPDOperationFixture(t)
	defer cancel()
	if _, err := repo.ClaimOperation(ctx, reserved.TenantID, reserved.ID,
		reserved.Version, "generic-writer", time.Minute); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("generic worker claimed reserved PD operation: %v", err)
	}
	after, err := repo.GetOperation(ctx, reserved.TenantID, reserved.ID)
	if err != nil || after.Status != operation.Pending || after.Version != reserved.Version {
		t.Fatalf("generic claim changed reserved operation: %+v %v", after, err)
	}
}

func TestGenericStartRejectsReservedPDOperationAfterValidLease(t *testing.T) {
	repo, ctx, cancel, reserved := reservedPDOperationFixture(t)
	defer cancel()
	// Isolated setup of an otherwise valid PD lease: never use generic
	// ClaimOperation to get it, because that is exactly the restricted path.
	leased, err := repo.mutateOperation(ctx, reserved.TenantID, reserved.ID,
		reserved.Version, func(current operation.Operation, now time.Time) (operation.Operation, error) {
			return current.Claim(reserved.Version, "pd-worker", now, time.Minute)
		})
	if err != nil || leased.Status != operation.Leased {
		t.Fatalf("cannot set up valid leased PD state: %+v %v", leased, err)
	}
	if _, err := repo.StartOperation(ctx, reserved.TenantID, reserved.ID,
		leased.Version, "pd-worker", leased.FenceToken); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("generic worker started reserved PD lease: %v", err)
	}
	after, err := repo.GetOperation(ctx, reserved.TenantID, reserved.ID)
	if err != nil || after.Status != operation.Leased ||
		after.Version != leased.Version || after.FenceToken != leased.FenceToken {
		t.Fatalf("generic start changed reserved lease: %+v %v", after, err)
	}
}
