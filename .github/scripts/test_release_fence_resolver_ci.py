"""Keep the database-derived release fence resolver as a non-skipped PostgreSQL source gate."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"


class ReleaseFenceResolverSourceCI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = WORKFLOW.read_text(encoding="utf-8")
        assert raw.count("\\n  release-fence-source:\\n") == 1
        cls.verify, cls.source = raw.split("\\n  release-fence-source:\\n", 1)

    def test_merged_tree_keeps_full_suite(self):
        self.assertIn("go test -race -count=1 ./...", self.verify)
        self.assertIn("Assert PR integration-tree checkout", self.verify)

    def test_exact_source_runs_database_derived_resolver_without_skip(self):
        expected = "go test -count=1 -v -run '^TestResolveReleaseCommitFence' ./internal/store/postgres | tee release-fence-resolver-pg.log"
        self.assertEqual(self.source.count(expected), 1)
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", self.source)
        self.assertIn("grep -F -- '--- PASS: TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows'", self.source)
        self.assertIn("grep -F -- '--- PASS: TestResolveReleaseCommitFenceRejectsMissingTransactionAndMalformedScope'", self.source)
        self.assertIn("if grep -F -- '--- SKIP: TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows'", self.source)
        self.assertIn("verified_release_fence_resolver_source_sha=", self.source)


if __name__ == "__main__":
    unittest.main()
