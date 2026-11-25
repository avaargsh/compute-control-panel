"""Keep DRA receipt snapshot acceptance on exact source with PostgreSQL subtests required."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"


class ReceiptSnapshotLookupSourceCI(unittest.TestCase):
    def test_exact_source_and_required_postgres_subtests(self):
        raw = WORKFLOW.read_text(encoding="utf-8")
        verify, source = raw.split("\n  release-fence-source:\n", 1)
        name = "Require tenant and Provider scoped receipt snapshot lookup (no skip)"
        self.assertIn("go test -race -count=1 ./...", verify)
        self.assertNotIn(name, verify)
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", source)
        self.assertIn("Assert exact source checkout for release fence", source)
        self.assertEqual(source.count(name), 1)
        step = source.split(name, 1)[1].split("\n      - name:", 1)[0]
        for marker in (
            "DATABASE_URL:",
            "set -euo pipefail",
            "go test -count=1 -v -run",
            "TestGetPreDeleteDRASnapshotForReceiptDistinguishesUnusableStoreFromUncorrelatedEvent",
            "TestGetPreDeleteDRASnapshotForReceiptIsTenantProviderAndIdentityScoped",
            "TestPreDeleteReceiptSnapshotLookupRejectsAmbiguousIdentity",
            "TestMemoryPreDeleteReceiptSnapshotLookupRejectsMalformedInput",
            'grep -F -- "--- PASS: $test "',
            'for test in TestGetPreDeleteDRASnapshotForReceiptIsTenantProviderAndIdentityScoped TestPreDeleteReceiptSnapshotLookupRejectsAmbiguousIdentity; do',
            'grep -F -- "--- PASS: $test/postgres "',
            "if grep -F -- '--- SKIP:'",
            "exit 1",
            "verified_dra_receipt_snapshot_lookup_source_sha=",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, step)


if __name__ == "__main__":
    unittest.main()
