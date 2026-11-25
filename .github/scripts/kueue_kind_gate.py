"""Classify the exact PR diff before running the disposable Kueue controller."""
import re
import subprocess
import sys


def git(*args):
    return subprocess.check_output(("git", *args))


def sha(value):
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError("missing or invalid event source SHA")
    return value


def documentation(path):
    return path == b"README.md" or (path.startswith(b"docs/") and path.endswith(b".md"))


def scope(event, base, head):
    if git("rev-parse", "--verify", "HEAD").strip().decode("ascii") != sha(head):
        raise ValueError("checkout does not match exact event source")
    if event in {"push", "workflow_dispatch"}:
        return "required"
    if event != "pull_request":
        raise ValueError("unknown event scope")
    # Three-dot GitHub PR diff, without rename detection: a move from code to
    # documentation still lists the removed code path and requires smoke.
    merge_base = git("merge-base", "--all", sha(base), head).splitlines()
    if len(merge_base) != 1:
        raise ValueError("comparison has no unique merge base")
    paths = git("diff", "--name-only", "-z", "--no-renames",
                merge_base[0].decode("ascii"), head, "--").split(b"\0")
    changed = [path for path in paths if path]
    if not changed:
        raise ValueError("empty PR comparison cannot establish documentation-only scope")
    return "unrelated" if all(map(documentation, changed)) else "required"


if __name__ == "__main__":
    try:
        command, event, base, head = sys.argv[1:]
        if command != "scope":
            raise ValueError("unknown command")
        print("kind=" + scope(event, base, head))
    except (ValueError, subprocess.CalledProcessError) as exc:
        sys.exit(f"unable to determine Kueue kind scope: {exc}")
