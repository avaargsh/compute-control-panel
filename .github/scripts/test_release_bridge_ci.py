"""Prevent CI string interpolation from corrupting the PostgreSQL source-head gate."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"


class ReleaseBridgeWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = WORKFLOW.read_text(encoding="utf-8")
        assert raw.count("\n  release-fence-source:\n") == 1
        cls.integration, cls.source = raw.split("\n  release-fence-source:\n", 1)

    def test_merged_tree_keeps_full_coverage(self):
        for required in ("Assert PR integration-tree checkout", "go test -race -count=1 ./...",
                         "TestSIGKILLRecoveryLeaseIsFencedBeforeInspection", "go run ./cmd/migrate"):
            with self.subTest(required=required):
                self.assertIn(required, self.integration)
        self.assertNotIn("TestCorrelatedReleasePreflightChecksCurrentPostgresRows", self.integration)

    def test_source_postgres_command_is_not_broken_by_js_dollar_quote(self):
        expected = "go test -count=1 -v -run '^TestCorrelatedReleasePreflightChecksCurrentPostgresRows$' ./internal/store/postgres | tee release-bridge-pg.log"
        self.assertEqual(self.source.count(expected), 1)
        self.assertIn("ref: " + "$" + "{{ github.event.pull_request.head.sha }}", self.source)
        self.assertIn("verified_release_bridge_source_sha=", self.source)
        self.assertIn("grep -F -- '--- PASS: TestCorrelatedReleasePreflightChecksCurrentPostgresRows'", self.source)
        self.assertIn("if grep -F -- '--- SKIP:'", self.source)

    def test_database_derived_release_fence_resolver_is_required_on_source_head(self):
        expected = "go test -count=1 -v -run '^TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows$' ./internal/store/postgres | tee release-fence-resolver-pg.log"
        self.assertEqual(self.source.count(expected), 1)
        self.assertIn("verified_release_fence_resolver_source_sha=", self.source)
        self.assertIn("grep -F -- '--- PASS: TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows'", self.source)
        self.assertIn("if grep -F -- '--- SKIP: TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows'", self.source)


if __name__ == "__main__":
    unittest.main()
