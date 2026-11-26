"""Check ``upathtools.fs`` sync notes against the installed and locked ``fsspec`` versions.

Prints:
  - ``FSSPEC_SYNC_VERSION`` from ``upathtools.fs._sync``
  - The installed ``fsspec`` version (``importlib.metadata.version("fsspec")``)
  - The ``fsspec`` version pinned in ``uv.lock``
  - Every ``upathtools.fs`` module declaring ``__fsspec_sync__`` and its upstream symbols
  - The GitHub compare URL whenever the synced version differs from the installed/locked version

Exits with code ``0`` when all versions match (and every module note matches
``FSSPEC_SYNC_VERSION``), or ``1`` otherwise.
"""

import argparse
import importlib
import importlib.metadata
from pathlib import Path
import pkgutil
import sys
import tomllib
from typing import TYPE_CHECKING

import upathtools.fs as core_fs
from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote


if TYPE_CHECKING:
    from collections.abc import Sequence


_COMPARE_BASE_URL = "https://github.com/fsspec/filesystem_spec/compare"


def read_locked_fsspec_version(lockfile: Path) -> str:
    """Return the single ``fsspec`` package version pinned in ``lockfile``."""
    data = tomllib.loads(lockfile.read_text(encoding="utf-8"))
    matches = [pkg["version"] for pkg in data.get("package", []) if pkg.get("name") == "fsspec"]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly 1 'fsspec' entry in {lockfile}, found {len(matches)}")
    return str(matches[0])


def collect_sync_notes() -> list[tuple[str, FsspecSyncNote]]:
    """Walk ``upathtools.fs`` and return sorted ``(module_name, FsspecSyncNote)`` pairs."""
    found: list[tuple[str, FsspecSyncNote]] = []
    for modinfo in pkgutil.walk_packages(core_fs.__path__, prefix=f"{core_fs.__name__}."):
        mod = importlib.import_module(modinfo.name)
        note = getattr(mod, "__fsspec_sync__", None)
        if isinstance(note, FsspecSyncNote):
            found.append((modinfo.name, note))
    found.sort(key=lambda item: item[0])
    return found


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``fsspec`` sync check CLI and return an exit status code."""
    default_lockfile = Path(__file__).resolve().parents[1] / "uv.lock"
    parser = argparse.ArgumentParser(
        description="Verify upathtools.fs __fsspec_sync__ notes against installed/locked fsspec.",
    )
    parser.add_argument(
        "--lockfile",
        type=Path,
        default=default_lockfile,
        help="Path to uv.lock (default: repository root uv.lock).",
    )
    args = parser.parse_args(argv)

    synced_version = FSSPEC_SYNC_VERSION
    installed_version = importlib.metadata.version("fsspec")
    locked_version = read_locked_fsspec_version(args.lockfile)
    notes = collect_sync_notes()

    print(f"FSSPEC_SYNC_VERSION: {synced_version}")
    print(f"Installed fsspec:    {installed_version}")
    print(f"Locked fsspec:       {locked_version}")
    print(f"\nSynced modules ({len(notes)}):")
    for mod_name, note in notes:
        version_suffix = "" if note.version == synced_version else f" [version={note.version}!]"
        print(f"  - {mod_name}{version_suffix}")
        for ref in note.upstream:
            print(f"      * {ref}")

    mismatched_notes = [mod for mod, note in notes if note.version != synced_version]
    versions_match = synced_version == installed_version == locked_version and not mismatched_notes

    if synced_version != installed_version:
        compare_url = f"{_COMPARE_BASE_URL}/{synced_version}...{installed_version}"
        print(f"\nCompare URL (synced -> installed): {compare_url}")
    if locked_version not in {synced_version, installed_version}:
        locked_url = f"{_COMPARE_BASE_URL}/{synced_version}...{locked_version}"
        print(f"Compare URL (synced -> locked):    {locked_url}")

    if not versions_match:
        if mismatched_notes:
            print(
                "\nModules whose __fsspec_sync__.version != FSSPEC_SYNC_VERSION: "
                + ", ".join(mismatched_notes)
            )
        return 1

    print("\nStatus: OK (synced, installed, and locked fsspec versions match)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
