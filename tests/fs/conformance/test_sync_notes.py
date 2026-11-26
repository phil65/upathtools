"""Automated sync-note verification gate for ``upathtools.fs``."""

import importlib
import importlib.metadata
import inspect
from pathlib import Path
import pkgutil
import subprocess
import sys
import tomllib

import fsspec

import upathtools.fs as core_fs
from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote


_REPO_ROOT = Path(__file__).resolve().parents[3]
_LOCKFILE = _REPO_ROOT / "uv.lock"
_CHECK_SCRIPT = _REPO_ROOT / "scripts" / "check_fsspec_sync.py"

# Every module under ``upathtools.fs`` that ports or adapts upstream ``fsspec`` logic
# must declare an ``__fsspec_sync__ = FsspecSyncNote(...)`` record.
REQUIRED_SYNC_MODULES: frozenset[str] = frozenset({
    "upathtools.fs.base",
    "upathtools.fs.batch",
    "upathtools.fs.cache",
    "upathtools.fs.glob",
    "upathtools.fs.implementations.dirfs",
    "upathtools.fs.implementations.local",
    "upathtools.fs.implementations.memory",
    "upathtools.fs.interop.from_fsspec",
    "upathtools.fs.interop.to_fsspec",
    "upathtools.fs.paths",
    "upathtools.fs.registry",
    "upathtools.fs.runner",
})


def _locked_fsspec_version() -> str:
    lock_data = tomllib.loads(_LOCKFILE.read_text(encoding="utf-8"))
    matches = [pkg["version"] for pkg in lock_data["package"] if pkg.get("name") == "fsspec"]
    assert len(matches) == 1, f"Expected 1 fsspec entry in {_LOCKFILE}, found {len(matches)}"
    return str(matches[0])


def _walk_core_fs_modules() -> dict[str, object]:
    modules: dict[str, object] = {}
    for modinfo in pkgutil.walk_packages(core_fs.__path__, prefix=f"{core_fs.__name__}."):
        modules[modinfo.name] = importlib.import_module(modinfo.name)
    return modules


def test_sync_version_matches_lockfile_and_installed_fsspec() -> None:
    """``FSSPEC_SYNC_VERSION`` matches both ``uv.lock`` and installed ``fsspec``."""
    locked = _locked_fsspec_version()
    installed = importlib.metadata.version("fsspec")
    assert locked == FSSPEC_SYNC_VERSION
    assert installed == FSSPEC_SYNC_VERSION


def test_required_modules_have_valid_sync_notes_and_upstream_symbols_resolve() -> None:
    """Every ``__fsspec_sync__`` note is valid and every upstream symbol resolves in ``fsspec``."""
    locked = _locked_fsspec_version()
    installed = importlib.metadata.version("fsspec")
    fsspec_root = Path(inspect.getfile(fsspec)).parent
    modules = _walk_core_fs_modules()

    missing_from_pkg = REQUIRED_SYNC_MODULES - set(modules)
    assert not missing_from_pkg, (
        f"Required modules not discovered in upathtools.fs: {missing_from_pkg}"
    )

    noted_modules: set[str] = set()
    for mod_name, mod in sorted(modules.items()):
        note = getattr(mod, "__fsspec_sync__", None)
        if note is None and not hasattr(mod, "__fsspec_sync__"):
            continue

        assert isinstance(note, FsspecSyncNote), (
            f"{mod_name}.__fsspec_sync__ must be FsspecSyncNote, got {type(note).__name__}"
        )
        assert note.version == FSSPEC_SYNC_VERSION == locked == installed, (
            f"{mod_name}.__fsspec_sync__.version={note.version!r} out of sync "
            f"(expected {FSSPEC_SYNC_VERSION!r}, locked={locked!r}, installed={installed!r})"
        )
        assert isinstance(note.upstream, tuple), f"{mod_name}: upstream must be a tuple"
        assert note.upstream, f"{mod_name}: upstream symbol tuple must not be empty"

        for ref in note.upstream:
            assert ":" in ref, f"{mod_name}: upstream entry missing ':': {ref!r}"
            rel_file, symbol_path = ref.split(":", 1)
            assert rel_file.startswith("fsspec/"), (
                f"{mod_name}: upstream file must start with 'fsspec/': {ref!r}"
            )
            assert rel_file.endswith(".py"), (
                f"{mod_name}: upstream file must end with '.py': {ref!r}"
            )
            assert symbol_path, f"{mod_name}: empty symbol in upstream entry: {ref!r}"

            upstream_file = fsspec_root / rel_file.removeprefix("fsspec/")
            assert upstream_file.is_file(), (
                f"{mod_name}: upstream file {rel_file!r} not found at {upstream_file}"
            )

            upstream_mod_name = rel_file.removesuffix(".py").replace("/", ".")
            target: object = importlib.import_module(upstream_mod_name)
            for attr in symbol_path.split("."):
                assert hasattr(target, attr), (
                    f"{mod_name}: could not resolve {attr!r} in upstream ref {ref!r} on {target!r}"
                )
                target = getattr(target, attr)

        noted_modules.add(mod_name)

    missing_notes = REQUIRED_SYNC_MODULES - noted_modules
    assert not missing_notes, (
        f"Ported modules missing __fsspec_sync__ note: {sorted(missing_notes)}"
    )


def test_check_fsspec_sync_cli_runs_and_lists_all_synced_modules() -> None:
    """``scripts/check_fsspec_sync.py`` exits 0 and prints versions and all synced modules."""
    assert _CHECK_SCRIPT.is_file(), f"Missing sync check script: {_CHECK_SCRIPT}"
    proc = subprocess.run(
        [sys.executable, str(_CHECK_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 0, (
        f"check_fsspec_sync.py failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert FSSPEC_SYNC_VERSION in proc.stdout
    for mod_name in sorted(REQUIRED_SYNC_MODULES):
        assert mod_name in proc.stdout, f"Module {mod_name!r} missing from CLI output"


def test_check_fsspec_sync_cli_exits_nonzero_and_prints_compare_url_on_mismatch(
    tmp_path: Path,
) -> None:
    """``scripts/check_fsspec_sync.py`` exits 1 and prints a GitHub compare URL on version drift."""
    fake_lock = tmp_path / "uv.lock"
    fake_lock.write_text('[[package]]\nname = "fsspec"\nversion = "2099.1.0"\n', encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, str(_CHECK_SCRIPT), "--lockfile", str(fake_lock)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 1
    expected_url = (
        f"https://github.com/fsspec/filesystem_spec/compare/{FSSPEC_SYNC_VERSION}...2099.1.0"
    )
    assert expected_url in proc.stdout
