"""Pin PR-scoped workflow cancellation without cancelling post-merge checks."""

import re
import unittest
from pathlib import Path


WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"


class PRCIConcurrencyTests(unittest.TestCase):
    def assert_concurrency(self, name, group, cancel):
        workflow = (WORKFLOWS / name).read_text(encoding="utf-8")
        blocks = re.findall(r"^concurrency:\n((?:  [^\n]*\n)+)", workflow, re.M)
        self.assertEqual(len(blocks), 1, f"{name}: one workflow-level group required")
        self.assertEqual(
            blocks[0].splitlines(),
            [f"  group: {group}", f"  cancel-in-progress: {cancel}"],
        )
        self.assertIn("\npermissions:\n  contents: read\n", workflow)

    def test_go_pr_scope_preserves_independent_push_runs(self):
        self.assert_concurrency(
            "go.yml",
            "ccp-go-${{ github.event_name }}-${{ github.event.pull_request.number || github.run_id }}",
            "${{ github.event_name == 'pull_request' }}",
        )
        workflow = (WORKFLOWS / "go.yml").read_text(encoding="utf-8")
        self.assertIn('  push:\n    branches: [main, "feat/go-foundation"]\n', workflow)
        self.assertIn("  pull_request:\n", workflow)

    def test_evidence_keeps_ready_and_edit_events(self):
        self.assert_concurrency(
            "main-pr-evidence.yml",
            "ccp-evidence-pr-${{ github.event.pull_request.number }}",
            "true",
        )
        workflow = (WORKFLOWS / "main-pr-evidence.yml").read_text(encoding="utf-8")
        self.assertIn("types: [opened, synchronize, reopened, edited, ready_for_review]", workflow)

    def test_migration_retains_trusted_pr_target_trigger(self):
        self.assert_concurrency(
            "migration-lineage.yml",
            "ccp-migration-pr-${{ github.event.pull_request.number }}",
            "true",
        )
        workflow = (WORKFLOWS / "migration-lineage.yml").read_text(encoding="utf-8")
        self.assertIn("  pull_request_target:\n", workflow)
        self.assertIn("types: [opened, reopened, synchronize, edited]", workflow)


if __name__ == "__main__":
    unittest.main()
