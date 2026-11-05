import unittest

from check_migration_compat import find_conflicts


def migration(name: str, sha: str = "a" * 40) -> dict:
    return {"path": "migrations/" + name, "sha": sha, "type": "blob"}


class MigrationCompatTests(unittest.TestCase):
    def test_current_provider_0005_conflicts_with_stacked_economics_0005(self):
        main = [migration("0005_provider_registry_drain.sql")]
        child = [migration("0005_allocation_usage_evidence_sets.sql", "b" * 40)]
        conflicts = find_conflicts(main, child)
        self.assertEqual(len(conflicts), 1)
        self.assertIn("0005", conflicts[0])

    def test_applied_same_filename_with_different_content_is_rejected(self):
        main = [migration("0005_provider_registry_drain.sql", "a" * 40)]
        child = [migration("0005_provider_registry_drain.sql", "c" * 40)]
        self.assertEqual(len(find_conflicts(main, child)), 1)

    def test_renumbered_new_migration_is_compatible(self):
        main = [migration("0005_provider_registry_drain.sql")]
        child = [
            migration("0005_provider_registry_drain.sql"),
            migration("0006_allocation_usage_evidence_sets.sql", "b" * 40),
        ]
        self.assertEqual(find_conflicts(main, child), [])

    def test_stacked_parent_without_new_main_migration_has_no_collision(self):
        main = [migration("0005_provider_registry_drain.sql")]
        child = [migration("0004_service_directory.sql", "b" * 40)]
        self.assertEqual(find_conflicts(main, child), [])

    def test_duplicate_versions_in_branch_are_rejected_even_if_main_lacks_version(self):
        child = [
            migration("0006_first.sql"),
            migration("0006_second.sql", "b" * 40),
        ]
        conflicts = find_conflicts([], child)
        self.assertEqual(len(conflicts), 1)
        self.assertIn("0006", conflicts[0])

    def test_unchanged_migration_and_non_migration_files_are_ignored(self):
        main = [migration("0005_provider_registry_drain.sql")]
        child = [
            migration("0005_provider_registry_drain.sql"),
            {"path": "migrations/tests/0005_fixture.sql", "sha": "b" * 40, "type": "blob"},
            {"path": "README.md", "sha": "c" * 40, "type": "blob"},
        ]
        self.assertEqual(find_conflicts(main, child), [])


if __name__ == "__main__":
    unittest.main()
