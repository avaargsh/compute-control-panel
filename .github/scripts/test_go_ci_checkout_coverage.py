"""Regression guards for Go CI's integration-tree and exact-source trust boundaries."""

import unittest
from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"


class GoWorkflowCheckoutContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")
        separator = "\n  release-fence-source:\n"
        assert cls.workflow.count(separator) == 1, "exactly one source-only job is required"
        cls.integration, cls.source = cls.workflow.split(separator, 1)

    def test_full_verify_uses_github_integration_tree(self):
        verify = self.integration
        self.assertIn("  verify:", verify)
        self.assertIn("Assert PR integration-tree checkout", verify)
        self.assertIn("EXPECTED_INTEGRATION_SHA: ${{ github.sha }}", verify)
        self.assertIn("refs/pull/*/merge", verify)
        self.assertNotIn("ref: ${{ github.event.pull_request.head.sha", verify)
        self.assertNotIn("Require real PostgreSQL release Generation/UID fence test", verify)

    def test_full_verify_keeps_migrations_race_and_recovery(self):
        verify = self.integration
        for required in (
            "go run ./cmd/migrate",
            "migrations/tests/0001_compute_core_constraints.sql",
            "go vet ./...",
            "go test -race -count=1 ./...",
            "TestSIGKILLRecoveryLeaseIsFencedBeforeInspection",
        ):
            with self.subTest(required=required):
                self.assertIn(required, verify)

    def test_source_job_pins_literal_head_and_is_pr_only(self):
        source = self.source
        self.assertIn("if: github.event_name == 'pull_request'", source)
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", source)
        self.assertIn("EXPECTED_SOURCE_SHA: ${{ github.event.pull_request.head.sha }}", source)
        self.assertIn("git rev-parse --verify HEAD", source)
        self.assertIn("verified_release_fence_source_sha=", source)
        self.assertNotIn("go test -race -count=1 ./...", source)

    def test_source_job_requires_live_postgres_non_skipped_fence(self):
        source = self.source
        for required in (
            "image: postgres:16-alpine",
            "go run ./cmd/migrate",
            "DATABASE_URL: postgres://postgres:compute-ci@127.0.0.1:5432/postgres?sslmode=disable",
            "TestReleaseCommitFenceMatchesLivePostgresGenerationAndUID",
            "grep -F -- '--- PASS: TestReleaseCommitFenceMatchesLivePostgresGenerationAndUID'",
            "grep -F -- '--- SKIP:'",
        ):
            with self.subTest(required=required):
                self.assertIn(required, source)


if __name__ == "__main__":
    unittest.main()
