"""Prevent PD source changes from silently skipping real JobSet/Kueue kind CI.

This tests workflow trigger selection, not physical GPU readiness. The JobSet
smoke remains a disposable simulated-DRA controller check.
"""

import fnmatch
import unittest
from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "kueue-kind-smoke.yml"
PD_SOURCE_PATHS = (
    "internal/pdstaging/stage.go",
    "internal/provider/pd_jobset_adapter.go",
    "internal/store/postgres/pd_staging_write_fence.go",
    "cmd/pd-jobset-provider-smoke/main.go",
    "scripts/pd_jobset_provider_kind_smoke.sh",
)


def event_paths(text: str, event: str) -> list[str]:
    """Read only a top-level GitHub event's paths list; no YAML dependency."""
    inside_event = False
    inside_paths = False
    patterns = []
    for line in text.splitlines():
        if line.startswith("  ") and not line.startswith("    "):
            inside_event = line == f"  {event}:"
            inside_paths = False
            continue
        if not inside_event:
            continue
        if line == "    paths:":
            inside_paths = True
            continue
        if inside_paths:
            if line.startswith("      - "):
                patterns.append(line[8:].strip().strip("'\""))
            elif line.strip():
                inside_paths = False
    return patterns


class PDKueueKindSourceCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_pd_source_changes_run_controller_smoke_on_pr_and_main(self):
        for event in ("pull_request", "push"):
            with self.subTest(event=event):
                patterns = event_paths(self.workflow, event)
                self.assertTrue(patterns, f"{event} must filter real paths")
                for path in PD_SOURCE_PATHS:
                    with self.subTest(event=event, source=path):
                        self.assertTrue(
                            any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns),
                            f"{event} skips PD source {path}",
                        )

    def test_smoke_still_executes_real_disposable_kind_workflow(self):
        for required in (
            "ref: ${{ github.event.pull_request.head.sha || github.sha }}",
            "Assert exact PR/source checkout identity",
            "CCP_DISPOSABLE_KIND_SMOKE: '1'",
            "bash scripts/kueue_kind_smoke.sh",
            "bash -n scripts/pd_jobset_dra_kind_smoke.sh",
            "bash -n scripts/pd_jobset_activation_gate_kind_smoke.sh",
            "bash -n scripts/pd_jobset_provider_kind_smoke.sh",
        ):
            with self.subTest(required=required):
                self.assertIn(required, self.workflow)


    def test_provider_smoke_is_built_and_executed_after_activation_fixture(self):
        runner = WORKFLOW.parents[2] / "scripts" / "kueue_kind_smoke.sh"
        text = runner.read_text(encoding="utf-8")
        self.assertIn('go build -trimpath -o "$dir/pd-jobset-provider-smoke" ./cmd/pd-jobset-provider-smoke', text)
        self.assertIn("COPY pd-jobset-provider-smoke /pd-jobset-provider-smoke", text)
        self.assertGreater(text.index("bash scripts/pd_jobset_provider_kind_smoke.sh"),
                           text.index("bash scripts/pd_jobset_activation_gate_kind_smoke.sh"))


if __name__ == "__main__":
    unittest.main()
