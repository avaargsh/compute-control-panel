package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func insertOutboxFixture(t *testing.T, db *sql.DB, tenant, aggregate string, version int64) string {
	t.Helper()
	id := "delivery-" + randomSuffix(t)
	_, err := db.ExecContext(context.Background(), `INSERT INTO compute_outbox_events (
		event_id,producer,event_type,schema_version,tenant_id,aggregate_type,
		aggregate_id,aggregate_version,occurred_at,correlation_id,payload)
		VALUES ($1,'compute-control-panel','compute.test.v1',1,$2,'compute_operation',
			$3,$4,clock_timestamp(),$5,'{}'::jsonb)`,
		id, tenant, aggregate, version, "corr-"+id)
	if err != nil { t.Fatal(err) }
	return id
}

func TestOutboxConcurrentClaimOneWinnerAndTenantIdentity(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tenant := "tenant-outbox-"+randomSuffix(t)
	id := insertOutboxFixture(t, db, tenant, "aggregate-"+randomSuffix(t), 1)
	const workers = 12
	claimed := make([]OutboxEvent, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i:=0; i<workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claimed[i], errs[i] = repo.ClaimOutbox(ctx, tenant, fmt.Sprintf("publisher-%d",i),time.Minute)
		}(i)
	}
	wg.Wait()
	n:=0
	for i,err:=range errs {
		if err==nil {
			if claimed[i].ID==id {
				n++
				if claimed[i].Attempts!=1 || claimed[i].TenantID=="" { t.Fatalf("invalid claim %+v",claimed[i]) }
			}
		} else if !errors.Is(err,sql.ErrNoRows) {t.Fatalf("worker %d: %v",i,err)}
	}
	if n!=1 { t.Fatalf("claimed event %q by %d workers",id,n) }
}

func TestOutboxCrashLeaseRecoveryFencesLateACK(t *testing.T) {
	db:=openIntegrationDB(t)
	repo,_:=NewRepository(db)
	ctx:=context.Background()
	tenant:="tenant-outbox-"+randomSuffix(t)
	id:=insertOutboxFixture(t,db,tenant,"aggregate-"+randomSuffix(t),1)
	first,err:=repo.ClaimOutbox(ctx,tenant,"dead-worker",40*time.Millisecond)
	if err!=nil || first.ID!=id { t.Fatalf("first claim %+v %v",first,err) }
	if err:=repo.MarkOutboxPublished(ctx,tenant,id,"other-worker",first.Attempts); !errors.Is(err,ErrOutboxLeaseLost) {
		t.Fatalf("wrong owner ACK accepted: %v",err)
	}
	if _,err:=repo.ClaimOutbox(ctx,tenant,"early-worker",time.Minute); !errors.Is(err,sql.ErrNoRows) {
		t.Fatalf("active lease stolen: %v",err)
	}
	time.Sleep(125*time.Millisecond)
	second,err:=repo.ClaimOutbox(ctx,tenant,"replacement",time.Minute)
	if err!=nil || second.ID!=id || second.Attempts!=first.Attempts+1 {
		t.Fatalf("reclaim %+v %v",second,err)
	}
	if err:=repo.MarkOutboxPublished(ctx,tenant,id,"dead-worker",first.Attempts); !errors.Is(err,ErrOutboxLeaseLost) {
		t.Fatalf("late ACK accepted: %v",err)
	}
	if err:=repo.MarkOutboxPublished(ctx,tenant,id,"replacement",second.Attempts); err!=nil {
		t.Fatalf("replacement ACK rejected: %v",err)
	}
	if err:=repo.MarkOutboxPublished(ctx,tenant,id,"replacement",second.Attempts); !errors.Is(err,ErrOutboxLeaseLost) {
		t.Fatalf("duplicate ACK accepted: %v",err)
	}
	var state string
	if err:=db.QueryRowContext(ctx,"SELECT delivery_state FROM compute_outbox_events WHERE event_id=$1",id).Scan(&state);err!=nil||state!="PUBLISHED" {
		t.Fatalf("durable ACK: %q %v",state,err)
	}
}

func TestOutboxBlocksLaterAggregateVersionButNotOtherTenant(t *testing.T) {
	db:=openIntegrationDB(t)
	repo,_:=NewRepository(db)
	ctx:=context.Background()
	aggregate:="aggregate-"+randomSuffix(t)
	tenantA:="tenant-a-"+randomSuffix(t)
	tenantB:="tenant-b-"+randomSuffix(t)
	firstID:=insertOutboxFixture(t,db,tenantA,aggregate,1)
	secondID:=insertOutboxFixture(t,db,tenantB,aggregate,1)
	first,err:=repo.ClaimOutbox(ctx,tenantA,"first",time.Minute)
	if err!=nil || first.ID!=firstID {t.Fatalf("first %+v %v",first,err)}
	second,err:=repo.ClaimOutbox(ctx,tenantB,"second",time.Minute)
	if err!=nil || second.ID!=secondID {t.Fatalf("other tenant blocked %+v %v",second,err)}
	if err:=repo.MarkOutboxPublished(ctx,tenantA,firstID,"first",first.Attempts);err!=nil {t.Fatal(err)}
	if err:=repo.MarkOutboxPublished(ctx,tenantB,secondID,"second",second.Attempts);err!=nil {t.Fatal(err)}
}

func TestOutboxSameAggregateHasContiguousDelivery(t *testing.T) {
	db:=openIntegrationDB(t)
	repo,_:=NewRepository(db)
	ctx:=context.Background()
	tenant:="tenant-sequence-"+randomSuffix(t)
	aggregate:="aggregate-"+randomSuffix(t)
	firstID:=insertOutboxFixture(t,db,tenant,aggregate,1)
	secondID:=insertOutboxFixture(t,db,tenant,aggregate,2)
	first,err:=repo.ClaimOutbox(ctx,tenant,"ordered-1",time.Minute)
	if err!=nil || first.ID!=firstID {t.Fatalf("first %+v %v",first,err)}
	if _,err:=repo.ClaimOutbox(ctx,tenant,"ordered-2",time.Minute); !errors.Is(err,sql.ErrNoRows) {
		t.Fatalf("next aggregate version dispatched before first ACK: %v",err)
	}
	if err:=repo.MarkOutboxPublished(ctx,tenant,firstID,"ordered-1",first.Attempts);err!=nil {t.Fatal(err)}
	second,err:=repo.ClaimOutbox(ctx,tenant,"ordered-2",time.Minute)
	if err!=nil || second.ID!=secondID {t.Fatalf("next event %+v %v",second,err)}
}

func TestOutboxRetryRetainsPayloadAndIsAtLeastOnce(t *testing.T) {
	db:=openIntegrationDB(t)
	repo,_:=NewRepository(db)
	ctx:=context.Background()
	tenant:="tenant-retry-"+randomSuffix(t)
	id:=insertOutboxFixture(t,db,tenant,"aggregate-"+randomSuffix(t),1)
	event,err:=repo.ClaimOutbox(ctx,tenant,"publisher-a",time.Minute)
	if err!=nil || event.ID!=id {t.Fatalf("claim %+v %v",event,err)}
	if err:=repo.RetryOutbox(ctx,tenant,id,"publisher-a",event.Attempts,0,"TEMPORARY_BROKER_FAILURE");err!=nil {t.Fatal(err)}
	reclaimed,err:=repo.ClaimOutbox(ctx,tenant,"publisher-b",time.Minute)
	if err!=nil || reclaimed.ID!=id || reclaimed.Attempts!=2 || string(reclaimed.Payload)!=string(event.Payload) {
		t.Fatalf("retry %+v %v",reclaimed,err)
	}
	if err:=repo.MarkOutboxPublished(ctx,tenant,id,"publisher-a",event.Attempts); !errors.Is(err,ErrOutboxLeaseLost) {
		t.Fatalf("old attempt ACK accepted %v",err)
	}
}
