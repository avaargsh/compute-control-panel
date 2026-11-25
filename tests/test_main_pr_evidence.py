import contextlib
import io
import json
import tempfile
import unittest
from unittest.mock import patch

from scripts.check_main_pr_evidence import main, validate

BASE = 'a' * 40
HEAD = 'b' * 40


def event(text, *, head=HEAD, base=BASE, draft=False, base_ref='main'):
    return {'pull_request': {
        'number': 44, 'draft': draft, 'body': text,
        'base': {'ref': base_ref, 'sha': base},
        'head': {'sha': head},
    }}


def body(*, base=BASE, head=HEAD):
    return f'''# Summary

Review-only change to governance checks.

## Governance metadata

- Target branch (main or internal feature parent); exact base/head commit SHAs: main base={base} head={head}
- Change class (docs-only / contract / migration / runtime / protected effect / provider): docs-only
- Affected domain / sole authoritative writer: ACP owns RunIntent; Runtime owns Run
- Governance gate(s): none; documentation only, G01-G05 not claimed

## Evidence (link real CI runs)

- Test/CI evidence at exact HEAD (run URL or explicit no-workflow reason): no workflow on prior main; test-only validation
- Authorization/recovery negative-case evidence or explained N/A: N/A because only documentation is changed

## Delivery / rollback

- Remaining blockers and explicit non-goals: G01-G05 live acceptance remains UNKNOWN
- Backward compatibility and rollback: revert this Markdown-only commit
- Main promotion vs internal-only merge decision, with rationale: main, documentation only and no runtime behavior
- Required GitHub branch rules/required status checks verified independently (or current status UNKNOWN): UNKNOWN, connector lacks ruleset read
'''


class EvidenceTests(unittest.TestCase):
    def test_complete_declaration(self):
        self.assertEqual(validate(event(body())), [])

    def test_draft_skips_without_promotion(self):
        self.assertEqual(validate(event('', draft=True)), [])

    def test_other_base_unaffected(self):
        self.assertEqual(validate(event('', base_ref='feature/safety')), [])

    def test_missing_base_ref_is_not_a_non_main_skip(self):
        payload = event('')
        del payload['pull_request']['base']['ref']
        self.assertTrue(validate(payload))

    def test_invalid_base_ref_is_not_a_non_main_skip(self):
        for ref in (None, '', ' ', ' main', 123):
            with self.subTest(ref=ref):
                self.assertTrue(validate(event('', base_ref=ref)))

    def test_missing_draft_flag_is_not_a_skip(self):
        payload = event('', base_ref='feature/safety')
        del payload['pull_request']['draft']
        self.assertTrue(validate(payload))

    def test_non_boolean_draft_flag_is_not_a_skip(self):
        for flag in (None, 'false', 0, 1):
            with self.subTest(flag=flag):
                self.assertTrue(validate(event('', base_ref='feature/safety', draft=flag)))

    def test_cli_distinguishes_skips_from_ready_validation(self):
        cases = (
            (event('', draft=True), 'skipped: draft'),
            (event('', base_ref='feature/safety'), 'skipped: non-main'),
            (event(body()), 'PR declaration complete'),
        )
        for payload, message in cases:
            with self.subTest(message=message):
                with tempfile.TemporaryDirectory() as directory:
                    path = directory + '/event.json'
                    with open(path, 'w', encoding='utf-8') as handle:
                        json.dump(payload, handle)
                    output = io.StringIO()
                    with patch('sys.argv', ['check_main_pr_evidence.py', path]), contextlib.redirect_stdout(output):
                        self.assertEqual(main(), 0)
                    self.assertIn(message, output.getvalue())

    def test_stale_head_rejected(self):
        self.assertTrue(any('head' in e.lower()
                            for e in validate(event(body(head='c' * 40)))))

    def test_stale_base_rejected(self):
        self.assertTrue(any('base' in e.lower()
                            for e in validate(event(body(base='c' * 40)))))

    def test_non_main_target_substring_rejected(self):
        text = body().replace('main base=', 'not-main base=')
        self.assertTrue(validate(event(text)))

    def test_duplicate_base_rejected(self):
        text = body().replace(f'head={HEAD}', f'head={HEAD} base={"c" * 40}')
        self.assertTrue(validate(event(text)))

    def test_duplicate_head_rejected(self):
        text = body().replace(f'head={HEAD}', f'head={HEAD} head={"c" * 40}')
        self.assertTrue(validate(event(text)))

    def test_blank_template_rejected(self):
        incomplete = '## Governance metadata\n\n- Affected domain / sole authoritative writer:\n'
        self.assertGreaterEqual(len(validate(event(incomplete))), 4)

    def test_todo_placeholder_rejected(self):
        text = body().replace('docs-only\n- Affected domain', 'TODO\n- Affected domain')
        self.assertTrue(validate(event(text)))

    def test_unexplained_na_rejected(self):
        text = body().replace('N/A because only documentation is changed', 'N/A')
        self.assertTrue(validate(event(text)))

    def test_missing_pr_fails_closed(self):
        self.assertTrue(validate({}))

    def test_malformed_head_fails_closed(self):
        self.assertTrue(validate(event(body(), head='invalid')))

    def test_unverified_rulesets_can_be_explicitly_unknown(self):
        self.assertEqual(validate(event(body())), [])


if __name__ == '__main__':
    unittest.main()
