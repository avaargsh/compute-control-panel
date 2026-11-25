"""Require the DRA snapshot read-time validation on the exact PR source."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "go.yml"
REGRESSION = "TestKueuePreDeleteSnapshotReadRevalidatesCreatePinAndStructure"


class PreDeleteSnapshotSourceCI(unittest.TestCase):
    def test_read_validation_is_executed_and_requires_pass(self):
        raw = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(raw.count("\n  release-fence-source:\n"), 1)
        source = raw.split("\n  release-fence-source:\n", 1)[1]
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", source)
        step = source.split("Require immutable pre-delete DRA snapshot PostgreSQL contract (no skip)", 1)[1]
        step = step.split("\n      - name:", 1)[0]
        command = next(line for line in step.splitlines() if "go test -count=1" in line)
        required = next(line for line in step.splitlines() if "for test in " in line)
        self.assertIn("|" + REGRESSION + ")$", command)
        self.assertIn(" " + REGRESSION + "; do", required)
        self.assertIn('grep -F -- "--- PASS: $test"', step)
        self.assertIn("if grep -F -- '--- SKIP:'", step)
        self.assertIn("verified_kueue_predelete_dra_snapshot_source_sha=", step)


if __name__ == "__main__":
    unittest.main()
