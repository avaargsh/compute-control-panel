package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

type observationAuth struct {
	p   WorkloadPrincipal
	err error
}

func (a observationAuth) Authenticate(context.Context, *http.Request) (WorkloadPrincipal, error) {
	return a.p, a.err
}

type observationAuthz struct {
	err   error
	calls int
}

func (a *observationAuthz) AuthorizeObservationRead(context.Context, WorkloadPrincipal) error {
	a.calls++
	return a.err
}

type observationSource struct {
	snapshot ObservationSnapshot
	err      error
	calls    int
	tenant   string
	projects []string
}

func (s *observationSource) ListObservations(_ context.Context, tenant string, projects []string) (ObservationSnapshot, error) {
	s.calls++
	s.tenant = tenant
	s.projects = append([]string(nil), projects...)
	return s.snapshot, s.err
}
func observationFixture() (ObservationReadDependencies, *observationSource, *observationAuthz) {
	at := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	src := &observationSource{snapshot: ObservationSnapshot{DataMode: observation.ModeLive, Items: []observation.Record{{
		TenantID: "tenant-a", ProjectID: "project-a", Resource: observation.ResourceRef{ClusterID: "cluster-a", Kind: "GPU", Name: "node-a/gpu-1"},
		Source: "kubernetes/device-plugin", Condition: observation.ConditionAvailable, ObservedAt: at, ExpiresAt: at.Add(time.Minute),
	}}}}
	authz := &observationAuthz{}
	deps := ObservationReadDependencies{Authenticator: observationAuth{p: WorkloadPrincipal{TenantID: "tenant-a", Subject: "spiffe://example.org/ns/ccp/sa/console", Service: "console", ProjectIDs: []string{"project-a"}}}, Authorizer: authz, Reader: src, Now: func() time.Time { return at.Add(30 * time.Second) }}
	return deps, src, authz
}
func observationRequest(h http.Handler, method, url string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, url, nil))
	return w
}
func TestNewObservationReadHandlerRequiresAllDependencies(t *testing.T) {
	deps, _, _ := observationFixture()
	if _, err := NewObservationReadHandler(ObservationReadDependencies{}); err == nil {
		t.Fatal("empty dependencies accepted")
	}
	deps.Reader = nil
	if _, err := NewObservationReadHandler(deps); err == nil {
		t.Fatal("nil reader accepted")
	}
}
func TestObservationReadLiveAndDemoUseSameSchema(t *testing.T) {
	for _, mode := range []observation.Mode{observation.ModeLive, observation.ModeDemo} {
		t.Run(string(mode), func(t *testing.T) {
			deps, src, authz := observationFixture()
			src.snapshot.DataMode = mode
			h, err := NewObservationReadHandler(deps)
			if err != nil {
				t.Fatal(err)
			}
			w := observationRequest(h, http.MethodGet, "/api/v1/observations")
			if w.Code != 200 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("observations must not be cached")
			}
			var payload struct {
				DataMode observation.Mode   `json:"data_mode"`
				Items    []observation.View `json:"items"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.DataMode != mode || len(payload.Items) != 1 || payload.Items[0].Status != observation.StatusAvailable {
				t.Fatalf("bad response %+v", payload)
			}
			if src.tenant != "tenant-a" || len(src.projects) != 1 || src.projects[0] != "project-a" || authz.calls != 1 {
				t.Fatalf("bad authorization scope reader=%+v", src)
			}
			if strings.Contains(w.Body.String(), "tenant-a") {
				t.Fatal("internal tenant identity leaked")
			}
		})
	}
}
func TestObservationReadFailsClosedForUnauthorizedOrMixedScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ObservationReadDependencies, *observationSource, *observationAuthz)
		status int
		reads  int
	}{
		{"unauthenticated", func(d *ObservationReadDependencies, _ *observationSource, _ *observationAuthz) {
			d.Authenticator = observationAuth{err: errors.New("no cert")}
		}, 401, 0},
		{"no project grants", func(d *ObservationReadDependencies, _ *observationSource, _ *observationAuthz) {
			d.Authenticator = observationAuth{p: WorkloadPrincipal{TenantID: "tenant-a", Subject: "service", Service: "console"}}
		}, 403, 0},
		{"denied", func(_ *ObservationReadDependencies, _ *observationSource, a *observationAuthz) {
			a.err = errors.New("confidential policy")
		}, 403, 0},
		{"cross tenant", func(_ *ObservationReadDependencies, s *observationSource, _ *observationAuthz) {
			s.snapshot.Items[0].TenantID = "other-tenant"
		}, 502, 1},
		{"cross project", func(_ *ObservationReadDependencies, s *observationSource, _ *observationAuthz) {
			s.snapshot.Items[0].ProjectID = "other-project"
		}, 502, 1},
		{"invalid mode", func(_ *ObservationReadDependencies, s *observationSource, _ *observationAuthz) {
			s.snapshot.DataMode = "surprise"
		}, 502, 1},
		{"read error", func(_ *ObservationReadDependencies, s *observationSource, _ *observationAuthz) {
			s.err = errors.New("internal storage secret")
		}, 502, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps, src, a := observationFixture()
			tc.mutate(&deps, src, a)
			h, err := NewObservationReadHandler(deps)
			if err != nil {
				t.Fatal(err)
			}
			w := observationRequest(h, http.MethodGet, "/api/v1/observations")
			if w.Code != tc.status || src.calls != tc.reads {
				t.Fatalf("status=%d,read calls=%d body=%s", w.Code, src.calls, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "other-tenant") || strings.Contains(w.Body.String(), "other-project") || strings.Contains(w.Body.String(), "confidential") || strings.Contains(w.Body.String(), "internal storage secret") {
				t.Fatalf("sensitive error leaked: %s", w.Body.String())
			}
		})
	}
}
func TestObservationReadShowsStaleNotGreen(t *testing.T) {
	deps, src, _ := observationFixture()
	deps.Now = func() time.Time { return src.snapshot.Items[0].ExpiresAt }
	h, err := NewObservationReadHandler(deps)
	if err != nil {
		t.Fatal(err)
	}
	w := observationRequest(h, "GET", "/api/v1/observations")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"STALE"`) {
		t.Fatalf("expired source shown as green: %d %s", w.Code, w.Body.String())
	}
}
// Even an empty literal '?' is query syntax; scope is server-owned and
// must not be interpreted differently by proxies or downstream services.
func TestObservationReadRejectsExplicitEmptyQuery(t *testing.T) {
	deps, src, _ := observationFixture()
	h, err := NewObservationReadHandler(deps)
	if err != nil {
		t.Fatal(err)
	}
	w := observationRequest(h, http.MethodGet, "/api/v1/observations?")
	if w.Code != http.StatusBadRequest || src.calls != 0 {
		t.Fatalf("explicit empty query escaped the read-only boundary: status=%d reads=%d body=%s", w.Code, src.calls, w.Body.String())
	}
}

func TestObservationReadRejectsWritesAndUnexpectedQuery(t *testing.T) {
	deps, src, _ := observationFixture()
	h, err := NewObservationReadHandler(deps)
	if err != nil {
		t.Fatal(err)
	}
	w := observationRequest(h, "POST", "/api/v1/observations")
	if w.Code != 405 || w.Header().Get("Allow") != "GET" || src.calls != 0 {
		t.Fatalf("write unexpectedly admitted: %+v", w)
	}
	w = observationRequest(h, "GET", "/api/v1/observations?project_id=other")
	if w.Code != 400 || src.calls != 0 {
		t.Fatalf("client-supplied scope admitted: %+v", w)
	}
}
