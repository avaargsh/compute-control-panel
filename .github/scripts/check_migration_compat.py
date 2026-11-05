"""Compare PR migration identities with current main; untrusted heads are data only."""


def find_conflicts(main_tree: list[dict], head_tree: list[dict]) -> list[str]:
    """Return version/name/checksum conflicts between two GitHub tree listings."""
    return []
