"""Read-only migration lineage check for all PRs, including stacked branches.

Only tree metadata is read from untrusted pull request heads. This program
must run from the default-branch code under pull_request_target, without
checking out or executing PR-controlled files or scripts.
"""

import json
import os
import re
import subprocess
import sys

MIGRATION_PATH = re.compile(r"^migrations/([0-9]{4})_[a-z0-9_]+\.sql$")


def _migration_index(tree: list[dict]) -> dict[str, list[tuple[str, str]]]:
    result: dict[str, list[tuple[str, str]]] = {}
    for entry in tree:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path")
        if not isinstance(path, str):
            continue
        match = MIGRATION_PATH.fullmatch(path)
        if not match:
            continue
        sha = entry.get("sha")
        if not isinstance(sha, str) or not sha:
            raise ValueError(f"missing Git blob SHA for {path}")
        result.setdefault(match.group(1), []).append((path, sha))
    return result


def find_conflicts(main_tree: list[dict], head_tree: list[dict]) -> list[str]:
    """Check unique versions and immutable main migration name/blob identity."""
    main = _migration_index(main_tree)
    head = _migration_index(head_tree)
    failures = []
    for label, source in (("main", main), ("PR head", head)):
        for version, entries in sorted(source.items()):
            if len(entries) != 1:
                failures.append(
                    f"{label}: duplicate migration version {version}: "
                    + ", ".join(p for p, _ in entries)
                )
    for version in sorted(main.keys() & head.keys()):
        main_path, main_sha = main[version][0]
        head_path, head_sha = head[version][0]
        if main_path != head_path:
            failures.append(
                f"migration version {version} collision: main has {main_path}, "
                f"PR head has {head_path}; preserve main and restack/renumber PR"
            )
        elif main_sha != head_sha:
            failures.append(
                f"migration version {version} rewrites immutable main blob "
                f"{main_path}: main={main_sha}, PR head={head_sha}"
            )
    return failures


def _github_tree(repo: str, ref: str) -> list[dict]:
    output = subprocess.run(
        ["gh", "api", f"repos/{repo}/git/trees/{ref}?recursive=1"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    payload = json.loads(output)
    if payload.get("truncated"):
        raise RuntimeError(f"GitHub returned a truncated tree for {repo}@{ref}")
    tree = payload.get("tree")
    if not isinstance(tree, list):
        raise RuntimeError(f"missing GitHub tree for {repo}@{ref}")
    return tree


def main() -> int:
    try:
        main_repo = os.environ["GITHUB_REPOSITORY"]
        head_repo = os.environ["PR_HEAD_REPOSITORY"]
        head_sha = os.environ["PR_HEAD_SHA"]
        if not re.fullmatch(r"[a-fA-F0-9]{40}", head_sha):
            raise ValueError("invalid PR head SHA")
        conflicts = find_conflicts(
            _github_tree(main_repo, "main"), _github_tree(head_repo, head_sha)
        )
    except (KeyError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"::error::Migration lineage check was not verifiable: {exc}")
        return 1
    if conflicts:
        for problem in conflicts:
            print(f"::error::{problem}")
        return 1
    print("Migration versions and applied-main blob identities are compatible.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
