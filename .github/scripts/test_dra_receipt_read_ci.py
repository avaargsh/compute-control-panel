"""CI contract: both merged-tree regression coverage and exact-source signed-receipt PG acceptance."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"

class DRAReceiptReadCIContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        text = WORKFLOW.read_text(encoding="utf-8")
        assert text.count("\n  release-fence-source:\n") == 1
        cls.verify, cls.source = text.split("\n  release-fence-source:\n", 1)

    def test_complete_merge_tree_suite(self):
        for required in ("Assert PR integration-tree checkout", "go test -race -count=1 ./...",
                         "TestSIGKILLRecoveryLeaseIsFencedBeforeInspection", "go run ./cmd/migrate"):
            with self.subTest(required=required):
                self.assertIn(required, self.verify)
        self.assertNotIn("Require real PostgreSQL recorded DRA receipt acceptance", self.verify)

    def test_exact_source_tenant_scope_and_auth_are_required(self):
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", self.source)
        step = self.source.split(
            "Require authenticated DRA receipt tenant isolation (no skip)", 1
        )[1].split("\n      - name:", 1)[0]
        required = (
            "TestDRAReceiptIngestRequiresVerifiedSPIFFEIdentityAndExactSource",
            "TestDRAReceiptIngestRejectsHistoricalEvidenceFromDifferentProvider",
            "TestDRAReceiptIngestRejectsHistoricalEvidenceFromDifferentTenant",
        )
        command = next(line for line in step.splitlines() if "go test -count=1" in line)
        pass_check = next(line for line in step.splitlines() if "for test in " in line)
        for name in required:
            with self.subTest(name=name):
                self.assertIn(name, command)
                self.assertIn(name, pass_check)
        self.assertIn('grep -F -- "--- PASS: $test "', step)
        self.assertIn("if grep -F -- '--- SKIP:'", step)
        self.assertIn("verified_dra_receipt_tenant_scope_source_sha=", step)

    def test_exact_source_receipt_tests(self):
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", self.source)
        self.assertIn("Require real PostgreSQL release Generation/UID fence test", self.source)
        self.assertIn("Require real PostgreSQL correlated release bridge acceptance", self.source)
        self.assertIn("TestRecordedDRAReceipt(SameTransactionPresenceReplayAndRevocation|RejectsUnrestrictedTestOnlyJournalIngress)$'", self.source)
        self.assertIn("--- PASS: TestRecordedDRAReceiptSameTransactionPresenceReplayAndRevocation", self.source)
        self.assertIn("--- PASS: TestRecordedDRAReceiptRejectsUnrestrictedTestOnlyJournalIngress", self.source)
        self.assertIn("if grep -F -- '--- SKIP: TestRecordedDRAReceipt'", self.source)

if __name__ == "__main__":
    unittest.main()
