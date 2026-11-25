"""Exercise the scope and actual workflow verdict without starting a cluster."""
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import kueue_kind_gate

HEAD = "a" * 40
BASE = "b" * 40
MERGE = "c" * 40
WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "kueue-kind-smoke.yml"


class ScopeTest(unittest.TestCase):
    def test_documentation_only_can_skip_kind(self):
        with patch.object(kueue_kind_gate, "git", side_effect=[
            HEAD.encode(), MERGE.encode(), b"README.md\0docs/quickstart.md\0"
        ]) as git:
            self.assertEqual(kueue_kind_gate.scope("pull_request", BASE, HEAD), "unrelated")
        self.assertIn("--no-renames", git.call_args.args)
        self.assertIn("-z", git.call_args.args)

    def test_relevant_paths_and_unfamiliar_paths_require_kind(self):
        for path in (b"internal/provider/kueue_adapter.go", b".github/workflows/kueue-kind-smoke.yml",
                     b"go.mod", b"unknown-binary\nartifact"):
            with self.subTest(path=path), patch.object(kueue_kind_gate, "git", side_effect=[
                HEAD.encode(), MERGE.encode(), b"docs/quickstart.md\0" + path + b"\0"
            ]):
                self.assertEqual(kueue_kind_gate.scope("pull_request", BASE, HEAD), "required")

    def test_unknown_or_stale_comparison_cannot_certify_scope(self):
        for result in ((BASE.encode(),), (HEAD.encode(), b""), (HEAD.encode(), b"1\n2\n"),
                       (HEAD.encode(), MERGE.encode(), b"")):
            with self.subTest(result=result), patch.object(kueue_kind_gate, "git", side_effect=result):
                with self.assertRaises(ValueError):
                    kueue_kind_gate.scope("pull_request", BASE, HEAD)
        with patch.object(kueue_kind_gate, "git", return_value=HEAD.encode()):
            with self.assertRaises(ValueError):
                kueue_kind_gate.scope("pull_request", "", HEAD)

    def test_push_and_dispatch_require_kind(self):
        for event in ("push", "workflow_dispatch"):
            with self.subTest(event=event), patch.object(kueue_kind_gate, "git", return_value=HEAD.encode()):
                self.assertEqual(kueue_kind_gate.scope(event, "", HEAD), "required")


class WorkflowTriggerTest(unittest.TestCase):
    def test_retargeting_pull_request_to_main_triggers_the_gate(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "types: [opened, synchronize, reopened, ready_for_review, edited]",
            source,
        )


class WorkflowVerdictTest(unittest.TestCase):
    def test_verdict_truth_table(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        step = source.split("      - name: Require successful relevant kind execution\n", 1)[1]
        recipe = step.split("        run: |\n", 1)[1]
        command = "\n".join(line[10:] for line in recipe.splitlines())
        for scope_result, kind_scope, kind_result, expected in (
            ("success", "required", "success", 0),
            ("success", "unrelated", "skipped", 0),
            ("success", "required", "skipped", 1),
            ("success", "required", "cancelled", 1),
            ("success", "required", "failure", 1),
            ("success", "unrelated", "cancelled", 1),
            ("failure", "", "skipped", 1),
            ("cancelled", "", "skipped", 1),
            ("success", "", "success", 1),
        ):
            with self.subTest(scope=scope_result, path=kind_scope, outcome=kind_result):
                result = subprocess.run(
                    ["bash", "-c", command],
                    env=dict(os.environ, SCOPE_RESULT=scope_result, KIND_SCOPE=kind_scope,
                             KIND_RESULT=kind_result),
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
