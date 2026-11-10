package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testKubeJob = `{"metadata":{"name":"job-a","namespace":"tenant-a","uid":"job-uid-a","resourceVersion":"11","annotations":{"compute-control-panel.io/tenant-id":"tenant-a","compute-control-panel.io/workload-id":"workload-a","compute-control-panel.io/desired-generation":"7"}}}`
const testKueueWorkloads = `{"metadata":{"resourceVersion":"21"},"items":[{"metadata":{"name":"kueue-a","namespace":"tenant-a","uid":"kueue-uid-a","resourceVersion":"22","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-uid-a","controller":true}]},"status":{"admission":{"clusterQueue":"cq-a"},"conditions":[{"type":"Admitted","status":"True"}]}}]}`
const testKubePods = `{"metadata":{"resourceVersion":"31"},"items":[{"metadata":{"name":"pod-a","namespace":"tenant-a","uid":"pod-uid-a","resourceVersion":"32","labels":{"batch.kubernetes.io/controller-uid":"job-uid-a"},"ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-uid-a","controller":true}]},"spec":{"nodeName":"node-a"},"status":{"conditions":[{"type":"PodScheduled","status":"True"}]}}]}`

func observedJobQuery() KubernetesJobQuery {
	return KubernetesJobQuery{
		TenantID: "tenant-a", WorkloadID: "workload-a",
		DesiredGeneration: 7, Namespace: "tenant-a",
		JobName: "job-a", ExpectedJobUID: "job-uid-a",
	}
}

func testKubeReader(t *testing.T, job, workloads, pods string, status int) (*KubernetesObservationReader, *int, func()) {
	t.Helper()
	reads := new(int)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*reads++
		if r.Method != http.MethodGet {
			t.Errorf("Kubernetes observer attempted mutating method: %s", r.Method)
			http.Error(w, "write forbidden", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/tenant-a/jobs/job-a":
			fmt.Fprint(w, job)
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
			if status != 0 {
				http.Error(w, "forbidden", status)
				return
			}
			fmt.Fprint(w, workloads)
		case "/api/v1/namespaces/tenant-a/pods":
			if r.URL.Query().Get("labelSelector") != "batch.kubernetes.io/controller-uid=job-uid-a" {
				t.Errorf("unexpected Pod selector %q", r.URL.RawQuery)
			}
			fmt.Fprint(w, pods)
		default:
			t.Errorf("unexpected Kubernetes path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	client := server.Client()
	client.Timeout = 3 * time.Second
	reader, err := NewKubernetesObservationReader(server.URL, client)
	if err != nil {
		server.Close()
		t.Fatalf("create read-only reader: %v", err)
	}
	return reader, reads, server.Close
}

func TestKubernetesObservationReaderStagesFromActualReadOnlyAPI(t *testing.T) {
	tests := []struct {
		name      string
		workloads string
		pods      string
		want      KueueObservationStage
	}{
		{"admitted and placed", testKueueWorkloads, testKubePods, KueuePodPlaced},
		{"admitted but pod absent", testKueueWorkloads, `{"items":[]}`, KueueAdmitted},
		{"kueue not admitted", strings.Replace(testKueueWorkloads, `"status":"True"`, `"status":"False"`, 1), `{"items":[]}`, KueueUnproven},
		{"no kueue workload", `{"items":[]}`, `{"items":[]}`, KueueUnproven},
		{"pod scheduled condition not ready", testKueueWorkloads, strings.Replace(testKubePods, `"status":"True"`, `"status":"False"`, 1), KueueAdmitted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader, reads, closeServer := testKubeReader(t, testKubeJob, tc.workloads, tc.pods, 0)
			defer closeServer()
			e, got, err := reader.ObserveJob(context.Background(), observedJobQuery())
			if err != nil || got != tc.want {
				t.Fatalf("observe stage=%q, err=%v, want=%q", got, err, tc.want)
			}
			if e.Job.UID != "job-uid-a" || *reads < 2 {
				t.Fatalf("missing authoritative Job GET evidence %+v, reads=%d", e.Job, *reads)
			}
		})
	}
}

func TestKubernetesObservationReaderFailsClosedOnIdentityOrPartialLists(t *testing.T) {
	tests := []struct {
		name      string
		job       string
		workloads string
		pods      string
		status    int
	}{
		{"successor Job UID", strings.Replace(testKubeJob, "job-uid-a", "job-uid-b", 1), testKueueWorkloads, testKubePods, 0},
		{"stale generation", strings.Replace(testKubeJob, `"desired-generation":"7"`, `"desired-generation":"6"`, 1), testKueueWorkloads, testKubePods, 0},
		{"cross tenant Job", strings.Replace(testKubeJob, `"tenant-id":"tenant-a"`, `"tenant-id":"tenant-b"`, 1), testKueueWorkloads, testKubePods, 0},
		{"foreign Kueue owner", testKubeJob, strings.Replace(testKueueWorkloads, "job-uid-a", "job-uid-b", 1), testKubePods, 0},
		{"incomplete Kueue list", testKubeJob, strings.Replace(testKueueWorkloads, `"resourceVersion":"21"`, `"continue":"next","resourceVersion":"21"`, 1), testKubePods, 0},
		{"foreign Pod owner", testKubeJob, testKueueWorkloads, strings.Replace(testKubePods, `"uid":"job-uid-a","controller"`, `"uid":"other-job","controller"`, 1), 0},
		{"incomplete Pod list", testKubeJob, testKueueWorkloads, strings.Replace(testKubePods, `"resourceVersion":"31"`, `"continue":"next","resourceVersion":"31"`, 1), 0},
		{"Kueue API forbidden", testKubeJob, testKueueWorkloads, testKubePods, http.StatusForbidden},
		{"duplicate Kueue owners", testKubeJob, strings.Replace(testKueueWorkloads, `]},"status"`, `]},"status"`, 1), testKubePods, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader, _, closeServer := testKubeReader(t, tc.job, tc.workloads, tc.pods, tc.status)
			defer closeServer()
			_, got, err := reader.ObserveJob(context.Background(), observedJobQuery())
			if err == nil || got != KueueUnproven {
				t.Fatalf("invalid observation trusted: stage=%q err=%v", got, err)
			}
		})
	}
}

func TestKubernetesObservationReaderRejectsUntrustedHTTPOrInvalidQuery(t *testing.T) {
	if _, err := NewKubernetesObservationReader("http://127.0.0.1:8080", &http.Client{Timeout: time.Second}); err == nil {
		t.Fatal("plaintext Kubernetes API unexpectedly accepted")
	}
	if _, err := NewKubernetesObservationReader("https://user:password@cluster.example", &http.Client{Timeout: time.Second}); err == nil {
		t.Fatal("URL credentials unexpectedly accepted")
	}
	if _, err := NewKubernetesObservationReader("https://cluster.example", &http.Client{}); err == nil {
		t.Fatal("unbounded client unexpectedly accepted")
	}
	reader, _, closeServer := testKubeReader(t, testKubeJob, testKueueWorkloads, testKubePods, 0)
	defer closeServer()
	q := observedJobQuery()
	q.Namespace = "../default"
	if _, stage, err := reader.ObserveJob(context.Background(), q); !errors.Is(err, ErrInvalidKueueObservation) || stage != KueueUnproven {
		t.Fatalf("unsafe namespace accepted: stage=%q err=%v", stage, err)
	}
}

func TestKubernetesObservationReaderDoesNotFollowKubernetesRedirect(t *testing.T) {
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("observer followed untrusted Kubernetes redirect")
	}))
	defer other.Close()
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer api.Close()
	client := api.Client()
	client.Timeout = time.Second
	reader, err := NewKubernetesObservationReader(api.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, stage, err := reader.ObserveJob(context.Background(), observedJobQuery()); err == nil || stage != KueueUnproven {
		t.Fatalf("redirect accepted: stage=%q err=%v", stage, err)
	}
}
