"""CI guards: same-tx signed DRA acceptance is source-pinned without losing merged-tree coverage."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"

class SignedSameTransactionSourceCI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = WORKFLOW.read_text(encoding="utf-8")
        assert raw.count("\n  release-fence-source:\n") == 1
        cls.verify, cls.source = raw.split("\n  release-fence-source:\n", 1)

    def test_merged_tree_suite_remains(self):
        for value in ("Assert PR integration-tree checkout", "go test -race -count=1 ./...",
                      "TestSIGKILLRecoveryLeaseIsFencedBeforeInspection", "go run ./cmd/migrate"):
            with self.subTest(value=value):
                self.assertIn(value, self.verify)
        self.assertNotIn("TestSignedRecordedAndCurrentGenerationOneTransaction", self.verify)

    def test_signed_same_tx_exact_source_non_skipped_pg(self):
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", self.source)
        self.assertIn("TestSignedRecordedAndCurrentGenerationOneTransaction$'", self.source)
        self.assertIn("--- PASS: TestSignedRecordedAndCurrentGenerationOneTransaction ", self.source)
        self.assertIn("if grep -F -- '--- SKIP: TestSignedRecordedAndCurrentGenerationOneTransaction'", self.source)
        self.assertIn("Require real PostgreSQL recorded DRA receipt acceptance", self.source)
        self.assertIn("Require real PostgreSQL correlated release bridge acceptance", self.source)

if __name__ == "__main__":
    unittest.main()
