#!/usr/bin/env python3
"""Validate declared evidence on main-bound PRs; not the truth of that evidence.

This is an advisory status check until the main branch Ruleset requires it.
No network, secrets, or GitHub write access is used.
"""

import json
import os
import re
import sys

SHA = re.compile(r'^[0-9a-f]{40}$')
HEADINGS = ('## Governance metadata', '## Evidence (link real CI runs)', '## Delivery / rollback')
FIELDS = (
    'Target branch ',
    'Change class ',
    'Affected domain / sole authoritative writer',
    'Governance gate(s)',
    'Test/CI evidence at exact HEAD ',
    'Authorization/recovery negative-case evidence or explained N/A',
    'Remaining blockers and explicit non-goals',
    'Backward compatibility and rollback',
    'Main promotion vs internal-only merge decision, with rationale',
    'Required GitHub branch rules/required status checks verified independently ',
)
EMPTY = {'', '-', 'todo', 'tbd', 'fixme', 'n/a', 'none', 'unknown', 'pending'}


def validate(event):
    """Return diagnostic failures, or [] for an adequately filled declaration."""
    pr = event.get('pull_request') if isinstance(event, dict) else None
    if not isinstance(pr, dict):
        return ['Missing or invalid pull_request webhook object']
    base, head = pr.get('base'), pr.get('head')
    if not isinstance(base, dict) or not isinstance(head, dict):
        return ['Missing GitHub base/head commit metadata']
    if base.get('ref') != 'main' or pr.get('draft') is True:
        return []

    errors = []
    expected_base, expected_head = base.get('sha'), head.get('sha')
    if not isinstance(expected_base, str) or not SHA.fullmatch(expected_base):
        errors.append('Invalid event base SHA')
    if not isinstance(expected_head, str) or not SHA.fullmatch(expected_head):
        errors.append('Invalid event head SHA')

    body = pr.get('body') or ''
    if not isinstance(body, str):
        return errors + ['PR body must be text']
    if len(body) > 65536:
        errors.append('PR body exceeds 64 KiB review limit')
    for heading in HEADINGS:
        if not re.search(r'^' + re.escape(heading) + r'\s*$', body, re.MULTILINE):
            errors.append('Required PR section missing: ' + heading)

    fields = {}
    for line in body.splitlines():
        if not line.startswith('- '):
            continue
        key, sep, value = line[2:].partition(':')
        if not sep:
            continue
        key = key.strip()
        for prefix in FIELDS:
            if key.startswith(prefix):
                fields.setdefault(prefix, []).append(value.strip())
                break

    for prefix in FIELDS:
        values = fields.get(prefix, [])
        if len(values) != 1:
            errors.append('Expected one field: ' + prefix.strip())
        elif values[0].strip().lower() in EMPTY:
            errors.append('Empty or unexplained placeholder field: ' + prefix.strip())

    binding = (fields.get('Target branch ') or [''])[0]
    declared_base = re.search(r'\bbase\s*=\s*([0-9a-f]{40})\b', binding)
    declared_head = re.search(r'\bhead\s*=\s*([0-9a-f]{40})\b', binding)
    if 'main' not in binding or not declared_base or declared_base.group(1) != expected_base:
        errors.append('PR base SHA/target does not match GitHub event main base')
    if not declared_head or declared_head.group(1) != expected_head:
        errors.append('PR head SHA does not match exact proposed commit')

    for prefix in ('Authorization/recovery negative-case evidence or explained N/A',
                   'Test/CI evidence at exact HEAD '):
        value = (fields.get(prefix) or [''])[0].strip()
        if re.fullmatch(r'(?i)(?:n/?a|not applicable|none|unknown)\W*', value):
            errors.append('Evidence needs explanation, not bare N/A/UNKNOWN: ' + prefix)
    return errors


def main():
    path = sys.argv[1] if len(sys.argv) == 2 else os.environ.get('GITHUB_EVENT_PATH')
    if not path:
        print('ERROR: missing GITHUB_EVENT_PATH', file=sys.stderr)
        return 2
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            event = json.load(handle)
    except (OSError, json.JSONDecodeError) as err:
        print('ERROR: invalid event payload: {}'.format(err), file=sys.stderr)
        return 2
    errors = validate(event)
    if errors:
        for error in errors:
            print('ERROR: ' + error, file=sys.stderr)
        return 1
    print('PR declaration complete; independent verification/Ruleset enforcement not implied')
    return 0


if __name__ == '__main__':
    sys.exit(main())
