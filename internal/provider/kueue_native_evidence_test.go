package provider

import (
	"context"
	"encoding/json"
	"testing"
)

const nativePendingReservation = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"False\",\"reason\":\"AdmissionCheckPending\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"
const nativeAdmittedReservation = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"True\",\"reason\":\"Admitted\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"
const nativeUnknownAdmission = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"Unknown\",\"reason\":\"AdmissionCheckPending\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"
const nativeMissingAdmission = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2}]}}]}"
const nativeUnreserved = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"False\",\"reason\":\"Inadmissible\",\"message\":\"insufficient quota\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\"}]}}]}"
const nativeMissingResourceUsage = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"False\",\"reason\":\"AdmissionCheckPending\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"
const nativeMissingAssignments = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\"},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"False\",\"reason\":\"AdmissionCheckPending\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"
const nativeDuplicateAssignments = "{\"metadata\":{\"resourceVersion\":\"21\"},\"items\":[{\"metadata\":{\"name\":\"kueue-a\",\"namespace\":\"tenant-a\",\"uid\":\"kueue-uid-a\",\"resourceVersion\":\"22\",\"ownerReferences\":[{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"uid\":\"job-uid-a\",\"controller\":true}]},\"status\":{\"admission\":{\"clusterQueue\":\"cq-a\",\"podSetAssignments\":[{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1},{\"name\":\"main\",\"flavors\":{\"cpu\":\"default\",\"memory\":\"default\"},\"resourceUsage\":{\"cpu\":\"2\",\"memory\":\"2Gi\"},\"count\":1}]},\"conditions\":[{\"type\":\"QuotaReserved\",\"status\":\"True\",\"reason\":\"QuotaReserved\",\"message\":\"reserved in cq-a\",\"lastTransitionTime\":\"2026-11-10T01:02:03Z\",\"observedGeneration\":2},{\"type\":\"Admitted\",\"status\":\"False\",\"reason\":\"AdmissionCheckPending\",\"message\":\"waiting for admission check\",\"lastTransitionTime\":\"2026-11-10T01:02:04Z\",\"observedGeneration\":2}]}}]}"

// A reservation and successful admission are distinct; preserve the native
// conditions and the exact resource quantities without deriving GPU readiness.
func TestKubernetesReaderPreservesNativeReservationEvidence(t *testing.T) {
	tests := []struct {
		name, workload, reserved, admitted string
		stage                              KueueObservationStage
	}{
		{"pending checks", nativePendingReservation, "True", "False", KueueUnproven},
		{"admitted", nativeAdmittedReservation, "True", "True", KueueAdmitted},
		{"unknown admission", nativeUnknownAdmission, "True", "Unknown", KueueUnproven},
		{"missing admission condition", nativeMissingAdmission, "True", "MISSING", KueueUnproven},
		{"quota denied", nativeUnreserved, "False", "MISSING", KueueUnproven},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, _, done := testKubeReader(t, testKubeJob, tt.workload, `{"items":[]}`, 0)
			defer done()
			evidence, stage, err := reader.ObserveJob(context.Background(), observedJobQuery())
			if err != nil || stage != tt.stage {
				t.Fatalf("stage=%s err=%v want=%s", stage, err, tt.stage)
			}
			if evidence.KueueQuotaCondition.Status != tt.reserved ||
				evidence.KueueAdmissionCondition.Status != tt.admitted {
				t.Fatalf("condition states collapsed: quota=%+v admission=%+v",
					evidence.KueueQuotaCondition, evidence.KueueAdmissionCondition)
			}
			if tt.reserved == "True" {
				if evidence.KueueQuotaCondition.Reason != "QuotaReserved" ||
					evidence.KueueQuotaCondition.Message != "reserved in cq-a" ||
					evidence.KueueQuotaCondition.LastTransitionTime != "2026-11-10T01:02:03Z" ||
					evidence.KueueQuotaCondition.ObservedGeneration != 2 {
					t.Fatalf("Kueue reason/time/generation dropped: %+v", evidence.KueueQuotaCondition)
				}
				var items []struct {
					Name          string            `json:"name"`
					Flavors       map[string]string `json:"flavors"`
					ResourceUsage map[string]string `json:"resourceUsage"`
					Count         *int32            `json:"count"`
				}
				if err := json.Unmarshal([]byte(evidence.KueuePodSetAssignmentsJSON), &items); err != nil ||
					len(items) != 1 || items[0].Name != "main" ||
					items[0].ResourceUsage["cpu"] != "2" ||
					items[0].ResourceUsage["memory"] != "2Gi" ||
					items[0].Flavors["cpu"] != "default" ||
					items[0].Count == nil || *items[0].Count != 1 {
					t.Fatalf("podSet reservation incomplete: items=%+v err=%v", items, err)
				}
			} else if evidence.KueuePodSetAssignmentsJSON != "" {
				t.Fatalf("unreserved workload exposed assignments: %s", evidence.KueuePodSetAssignmentsJSON)
			}
		})
	}
}

func TestKubernetesReaderRejectsIncompleteReservedAssignments(t *testing.T) {
	for _, tt := range []struct{ name, workload string }{
		{"missing resource usage", nativeMissingResourceUsage},
		{"missing podset assignments", nativeMissingAssignments},
		{"duplicate podset name", nativeDuplicateAssignments},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader, _, done := testKubeReader(t, testKubeJob, tt.workload, `{"items":[]}`, 0)
			defer done()
			evidence, stage, err := reader.ObserveJob(context.Background(), observedJobQuery())
			if err == nil || stage != KueueUnproven || evidence != (KueueJobEvidence{}) {
				t.Fatalf("incomplete reservation trusted: evidence=%+v stage=%s err=%v", evidence, stage, err)
			}
		})
	}
}
