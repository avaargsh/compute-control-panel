from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

CONTRACT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = CONTRACT_ROOT / "schemas"
SCHEMA_BASE = "https://schemas.compute-control-panel.dev/v1/"

SHA256 = "sha256:" + ("a" * 64)
OBSERVED_AT = "2026-11-04T12:00:00Z"


class ContractSchemasTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas: dict[str, dict[str, Any]] = {}
        resources = []

        for path in sorted(SCHEMA_ROOT.glob("*.schema.json")):
            schema = json.loads(path.read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)
            schema_id = schema.get("$id")
            if not isinstance(schema_id, str) or not schema_id.startswith(SCHEMA_BASE):
                raise AssertionError(f"{path.name} must have a stable v1 $id")
            cls.schemas[path.name] = schema
            resources.append((schema_id, Resource.from_contents(schema)))

        if not cls.schemas:
            raise AssertionError("no JSON Schema contract files found")

        cls.registry = Registry().with_resources(resources)
        cls.format_checker = FormatChecker()

    @classmethod
    def validator(cls, name: str) -> Draft202012Validator:
        return Draft202012Validator(
            cls.schemas[name],
            registry=cls.registry,
            format_checker=cls.format_checker,
        )

    def test_all_contract_schemas_are_draft_2020_12_valid(self) -> None:
        self.assertGreaterEqual(len(self.schemas), 8)

    def test_owner_ref_accepts_runtime_run_and_sandbox_claim(self) -> None:
        validate = self.validator("owner-ref-v1.schema.json")
        validate.validate({
            "service": "agent-runtime",
            "resource_type": "run",
            "resource_id": "run-001",
        })
        validate.validate({
            "service": "sandbox-provider",
            "resource_type": "sandbox-claim",
            "resource_id": "claim-001",
        })

    def test_owner_ref_rejects_run_intent_and_unknown_owner(self) -> None:
        validate = self.validator("owner-ref-v1.schema.json")
        for owner in (
            {"service": "agent-control-panel", "resource_type": "run-intent", "resource_id": "intent-001"},
            {"service": "agent-runtime", "resource_type": "sandbox-claim", "resource_id": "claim-001"},
            {"service": "provider", "resource_type": "job", "resource_id": "job-001"},
        ):
            with self.subTest(owner=owner):
                with self.assertRaises(ValidationError):
                    validate.validate(owner)

    def test_workload_create_accepts_exactly_one_resource_mode(self) -> None:
        validate = self.validator("compute-workload-create-v1.schema.json")
        base = {
            "api_version": "compute.platform/v1",
            "project_id": "project-001",
            "owner_ref": {
                "service": "agent-runtime",
                "resource_type": "run",
                "resource_id": "run-001",
            },
        }
        validate.validate({**base, "compute_profile_ref": {"id": "gpu", "revision": "3"}})
        validate.validate({**base, "resources": {"cpu": "8", "memory": "32Gi"}})

        with self.assertRaises(Exception):
            validate.validate({
                **base,
                "compute_profile_ref": {"id": "gpu", "revision": "3"},
                "resources": {"cpu": "8", "memory": "32Gi"},
            })
        with self.assertRaises(Exception):
            validate.validate(base)

    def test_workload_create_rejects_client_selected_provider_and_tenant(self) -> None:
        validate = self.validator("compute-workload-create-v1.schema.json")
        payload = {
            "api_version": "compute.platform/v1",
            "project_id": "project-001",
            "owner_ref": {
                "service": "agent-runtime",
                "resource_type": "run",
                "resource_id": "run-001",
            },
            "compute_profile_ref": {"id": "gpu", "revision": "3"},
            "provider_id": "provider-from-client",
            "tenant_id": "tenant-from-client",
        }
        with self.assertRaises(Exception):
            validate.validate(payload)

    def test_acceptance_response_distinguishes_workload_from_allocation_phase(self) -> None:
        self.validator("compute-workload-accepted-v1.schema.json").validate({
            "workload_id": "workload-001",
            "desired_generation": 1,
            "desired_state": "ACTIVE",
            "observed_phase": "ACCEPTED",
            "allocation_id": "allocation-001",
            "allocation_phase": "PENDING",
            "compute_operation_id": "operation-001",
            "status_url": "/api/v1/workloads/workload-001",
        })

    def test_initial_allocation_is_pending_and_has_stable_identity(self) -> None:
        self.validator("compute-allocation-v1.schema.json").validate({
            "allocation_id": "allocation-001",
            "workload_id": "workload-001",
            "provider_id": "provider-001",
            "pool_id": "pool-001",
            "desired_generation": 1,
            "phase": "PENDING",
            "version": 1,
        })

    def test_initial_operation_is_allocation_bound_and_pending(self) -> None:
        self.validator("compute-operation-v1.schema.json").validate({
            "compute_operation_id": "operation-001",
            "tenant_id": "tenant-001",
            "workload_id": "workload-001",
            "allocation_id": "allocation-001",
            "provider_id": "provider-001",
            "operation_kind": "CREATE",
            "desired_generation": 1,
            "provider_request_digest": SHA256,
            "idempotency_key": "compute-operation-key-001",
            "version": 1,
            "fence_token": 0,
            "status": "PENDING",
        })


    def test_operation_schema_rejects_inconsistent_status_evidence(self) -> None:
        validate = self.validator("compute-operation-v1.schema.json")
        base = {
            "compute_operation_id": "operation-001",
            "tenant_id": "tenant-001",
            "workload_id": "workload-001",
            "allocation_id": "allocation-001",
            "provider_id": "provider-001",
            "operation_kind": "CREATE",
            "desired_generation": 1,
            "provider_request_digest": SHA256,
            "idempotency_key": "compute-operation-key-001",
            "version": 1,
            "fence_token": 0,
            "status": "PENDING",
        }
        invalid = [
            {**base, "lease_owner": "worker-1"},
            {**base, "cleanup_required": True},
            {**base, "operation_kind": "UPDATE"},
            {**base, "status": "FAILED"},
            {**base, "status": "MANUAL_REVIEW"},
            {**base, "status": "LEASED", "lease_owner": "worker-1", "lease_expires_at": OBSERVED_AT},
        ]
        for operation in invalid:
            with self.subTest(operation=operation):
                with self.assertRaises(ValidationError):
                    validate.validate(operation)

    def test_operation_schema_accepts_verified_create_and_rejects_delete_success_with_resource(self) -> None:
        validate = self.validator("compute-operation-v1.schema.json")
        succeeded_create = {
            "compute_operation_id": "operation-002",
            "tenant_id": "tenant-001",
            "workload_id": "workload-001",
            "allocation_id": "allocation-001",
            "provider_id": "provider-001",
            "operation_kind": "CREATE",
            "desired_generation": 1,
            "provider_request_digest": SHA256,
            "idempotency_key": "compute-operation-key-002",
            "version": 3,
            "fence_token": 1,
            "status": "SUCCEEDED",
            "provider_resource_id": "job-001",
            "provider_resource_uid": "uid-001",
        }
        validate.validate(succeeded_create)

        invalid_delete_success = {
            **succeeded_create,
            "operation_kind": "DELETE",
            "target_resource_id": "job-001",
            "target_resource_uid": "uid-001",
        }
        with self.assertRaises(ValidationError):
            validate.validate(invalid_delete_success)

    def test_resource_observation_is_timestamped_evidence(self) -> None:
        self.validator("compute-resource-observation-v1.schema.json").validate({
            "observation_id": "observation-001",
            "tenant_id": "tenant-001",
            "workload_id": "workload-001",
            "allocation_id": "allocation-001",
            "provider_id": "provider-001",
            "desired_generation": 1,
            "resource_kind": "kubernetes.job",
            "provider_resource_id": "job-001",
            "provider_resource_uid": "uid-001",
            "result": "READY",
            "source": "kubernetes-api",
            "adapter_version": "test-adapter/1",
            "observed_at": OBSERVED_AT,
            "response_digest": SHA256,
        })

    def test_event_envelope_accepts_v1_workload_accepted_event(self) -> None:
        self.validator("compute-event-envelope-v1.schema.json").validate({
            "event_id": "event-001",
            "event_type": "compute.workload.accepted.v1",
            "schema_version": 1,
            "producer": "compute-control-panel",
            "tenant_id": "tenant-001",
            "aggregate_type": "compute_workload",
            "aggregate_id": "workload-001",
            "aggregate_version": 1,
            "occurred_at": OBSERVED_AT,
            "correlation_id": "correlation-001",
            "data": {
                "workload_id": "workload-001",
                "allocation_id": "allocation-001",
                "operation_id": "operation-001",
                "desired_generation": 1,
            },
        })

    def test_openapi_matches_implemented_workload_read_and_auth_boundary(self) -> None:
        document = json.loads((CONTRACT_ROOT / "openapi.json").read_text(encoding="utf-8"))
        paths = document["paths"]
        get_workload = paths["/api/v1/workloads/{workload_id}"]["get"]
        response_ref = get_workload["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        self.assertEqual(response_ref, "#/components/schemas/WorkloadDetails")

        details = self.validator("compute-workload-details-v1.schema.json")
        details.validate({
            "workload": {
                "workload_id": "workload-001",
                "project_id": "project-001",
                "desired_generation": 1,
                "version": 1,
                "desired_state": "ACTIVE",
                "observed_phase": "ACCEPTED",
                "latest_allocation_id": "allocation-001",
                "created_at": OBSERVED_AT,
                "updated_at": OBSERVED_AT,
            },
            "allocations": [{
                "allocation_id": "allocation-001",
                "provider_id": "provider-001",
                "pool_id": "pool-001",
                "desired_generation": 1,
                "phase": "PENDING",
                "version": 1,
            }],
            "operations": [{
                "compute_operation_id": "operation-001",
                "allocation_id": "allocation-001",
                "provider_id": "provider-001",
                "operation_kind": "CREATE",
                "desired_generation": 1,
                "version": 1,
                "status": "PENDING",
            }],
            "history_truncated": False,
        })
        with self.assertRaises(ValidationError):
            details.validate({"workload_id": "workload-001"})

        self.assertIn("ServiceMTLS", document["components"]["securitySchemes"])
        self.assertEqual(document["components"]["securitySchemes"]["ServiceMTLS"]["type"], "mutualTLS")
        for route, method in (
            ("/api/v1/workloads", "post"),
            ("/api/v1/workloads/{workload_id}", "get"),
        ):
            self.assertEqual(paths[route][method]["security"], [{"ServiceMTLS": []}])
            self.assertEqual(paths[route][method]["x-implementation-status"], "handler-unmounted")
        for route, method in (
            ("/api/v1/workloads", "get"),
            ("/api/v1/workloads/{workload_id}", "delete"),
            ("/api/v1/workloads/{workload_id}/allocations", "get"),
            ("/api/v1/compute-operations/{compute_operation_id}", "get"),
        ):
            self.assertEqual(paths[route][method]["x-implementation-status"], "proposed")

    def test_openapi_declares_write_and_read_contract_paths_and_local_schema_refs(self) -> None:
        openapi_path = CONTRACT_ROOT / "openapi.json"
        document = json.loads(openapi_path.read_text(encoding="utf-8"))
        self.assertEqual(document["openapi"], "3.1.0")
        paths = document["paths"]
        expected = {
            ("/api/v1/workloads", "post"),
            ("/api/v1/workloads", "get"),
            ("/api/v1/workloads/{workload_id}", "get"),
            ("/api/v1/workloads/{workload_id}", "delete"),
            ("/api/v1/workloads/{workload_id}/allocations", "get"),
            ("/api/v1/compute-operations/{compute_operation_id}", "get"),
        }
        actual = {
            (path, method)
            for path, path_item in paths.items()
            for method in ("get", "post", "delete", "put", "patch")
            if method in path_item
        }
        self.assertTrue(expected.issubset(actual))

        create = paths["/api/v1/workloads"]["post"]
        idempotency = next(p for p in create["parameters"] if p["name"] == "Idempotency-Key")
        self.assertTrue(idempotency["required"])
        self.assertIn("202", create["responses"])
        self.assertIn("ServiceMTLS", create["security"][0])

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                ref = value.get("$ref")
                if isinstance(ref, str) and ref.startswith("./"):
                    target = (CONTRACT_ROOT / ref).resolve()
                    self.assertTrue(target.is_file(), f"unresolved OpenAPI reference: {ref}")
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(document)


if __name__ == "__main__":
    unittest.main()
