"""Sync metadata tracking upstream ``fsspec`` versions and symbols."""

from dataclasses import dataclass


FSSPEC_SYNC_VERSION = "2026.9.0"


@dataclass(frozen=True, slots=True)
class FsspecSyncNote:
    """Provenance record linking a core module to upstream ``fsspec`` symbols.

    Attributes:
        version: ``fsspec`` release version the ported logic was audited against.
        upstream: Upstream symbol references formatted as ``"fsspec/<file>.py:<symbol>"``.
        notes: Optional explanation of intentional divergences or porting details.
    """

    version: str
    upstream: tuple[str, ...]
    notes: str = ""
