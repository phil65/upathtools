from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
import importlib.metadata
from pathlib import Path
import tomllib
from typing import Any

import pytest

import upathtools.fs as core_fs
from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    FileInfo,
    FsspecSyncNote,
    GrepMatch,
)


def test_file_info_is_frozen_slots_kw_only() -> None:
    info = FileInfo(path="a/b.txt", type="file", size=12)

    with pytest.raises(FrozenInstanceError):
        info.size = 99  # type: ignore[misc]

    assert not hasattr(info, "__dict__")

    with pytest.raises(TypeError):
        FileInfo("a/b.txt", "file")  # type: ignore[misc]

    # No dict/mapping compatibility
    assert not hasattr(info, "__getitem__")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("a/b/c.txt", "c.txt"),
        ("a/b/", "b"),
        ("/", ""),
        ("", ""),
        ("solo.txt", "solo.txt"),
        ("///", ""),
    ],
)
def test_file_info_name(path: str, expected: str) -> None:
    info = FileInfo(path=path, type="file")
    assert info.name == expected


def test_file_info_is_file_and_is_dir() -> None:
    file_info = FileInfo(path="a.txt", type="file")
    dir_info = FileInfo(path="dir", type="directory")
    link_info = FileInfo(path="link", type="symlink", is_symlink=True)
    other_info = FileInfo(path="fifo", type="other")

    assert file_info.is_file is True
    assert file_info.is_dir is False

    assert dir_info.is_file is False
    assert dir_info.is_dir is True

    assert link_info.is_file is False
    assert link_info.is_dir is False

    assert other_info.is_file is False
    assert other_info.is_dir is False


def test_file_info_extra_is_read_only_and_not_aliased() -> None:
    default_info = FileInfo(path="a.txt", type="file")
    assert default_info.extra == {}
    with pytest.raises(TypeError):
        default_info.extra["k"] = "v"  # type: ignore[index]

    caller_dict: dict[str, Any] = {"storage_class": "STANDARD", "version": 1}
    info = FileInfo(path="a.txt", type="file", extra=caller_dict)

    caller_dict["storage_class"] = "GLACIER"
    caller_dict["injected"] = True

    assert info.extra == {"storage_class": "STANDARD", "version": 1}
    with pytest.raises(TypeError):
        info.extra["storage_class"] = "MUTATED"  # type: ignore[index]


@pytest.mark.parametrize(
    "original",
    [
        FileInfo(path="simple.txt", type="file"),
        FileInfo(path="folder", type="directory", size=0),
        FileInfo(
            path="a/b/c.bin",
            type="file",
            size=4096,
            mtime=1700000000.5,
            ctime=1699999000.25,
            mode=0o100644,
            etag='"deadbeef"',
            is_symlink=True,
            symlink_target="a/b/target.bin",
            extra={"owner": "alice", "generation": 42},
        ),
        FileInfo(
            path="a/link",
            type="symlink",
            is_symlink=True,
            symlink_target="../real",
        ),
    ],
)
def test_file_info_fsspec_dict_round_trip(original: FileInfo) -> None:
    as_dict = original.to_fsspec_dict()
    restored = FileInfo.from_fsspec_dict(as_dict)
    assert restored == original


def test_from_fsspec_dict_s3_style() -> None:
    last_modified = datetime(2025, 6, 15, 12, 30, 45, tzinfo=UTC)
    expected_size = 1024
    raw = {
        "Key": "my-bucket/prefix/data.parquet",
        "LastModified": last_modified,
        "ETag": '"9b2cf535f27731c974343645a3985328"',
        "size": expected_size,
        "type": "file",
        "StorageClass": "STANDARD",
    }
    snapshot = dict(raw)

    info = FileInfo.from_fsspec_dict(raw)

    assert raw == snapshot  # input not mutated
    assert info.path == "my-bucket/prefix/data.parquet"
    assert info.name == "data.parquet"
    assert info.type == "file"
    assert info.size == expected_size
    assert info.mtime == last_modified.timestamp()
    assert info.etag == '"9b2cf535f27731c974343645a3985328"'
    assert info.extra == {"StorageClass": "STANDARD"}
    with pytest.raises(TypeError):
        info.extra["StorageClass"] = "DEEP_ARCHIVE"  # type: ignore[index]

    # S3 dict with "name" instead of "Key"
    named_raw = {**raw, "name": "my-bucket/other.parquet"}
    named_raw.pop("Key")
    named_info = FileInfo.from_fsspec_dict(named_raw)
    assert named_info.path == "my-bucket/other.parquet"


def test_from_fsspec_dict_gcs_style() -> None:
    expected_size = 256
    raw = {
        "name": "gcs-bucket/logs/2025.json",
        "updated": "2025-03-20T14:22:10.500000Z",
        "CreationTime": "2025-03-20T14:00:00+02:00",
        "size": expected_size,
        "type": "file",
        "etag": "CKi789==",
        "generation": "1710944530500000",
    }

    info = FileInfo.from_fsspec_dict(raw)

    assert info.path == "gcs-bucket/logs/2025.json"
    assert info.type == "file"
    assert info.size == expected_size
    assert info.mtime == datetime(2025, 3, 20, 14, 22, 10, 500000, tzinfo=UTC).timestamp()
    expected_ctime = datetime(2025, 3, 20, 14, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    assert info.ctime == expected_ctime.timestamp()
    assert info.etag == "CKi789=="
    assert info.extra == {"generation": "1710944530500000"}


def test_from_fsspec_dict_local_style() -> None:
    expected_size = 64
    expected_mtime = 1700000000.25
    expected_ctime = 1699999000.0
    expected_mode = 0o100644
    raw = {
        "name": "/var/data/link.txt",
        "type": "file",
        "size": expected_size,
        "mtime": expected_mtime,
        "created": expected_ctime,
        "mode": expected_mode,
        "islink": True,
        "destination": "/var/data/target.txt",
        "uid": 1000,
        "gid": 1000,
    }

    info = FileInfo.from_fsspec_dict(raw)

    assert info.path == "/var/data/link.txt"
    assert info.type == "file"
    assert info.size == expected_size
    assert info.mtime == expected_mtime
    assert info.ctime == expected_ctime
    assert info.mode == expected_mode
    assert info.is_symlink is True
    assert info.symlink_target == "/var/data/target.txt"
    assert info.extra == {"uid": 1000, "gid": 1000}


def test_from_fsspec_dict_type_normalization() -> None:
    dir_info = FileInfo.from_fsspec_dict({"name": "d", "type": "directory"})
    assert dir_info.type == "directory"
    assert dir_info.is_dir is True

    link_info = FileInfo.from_fsspec_dict({"name": "l", "type": "link", "destination": "t"})
    assert link_info.type == "symlink"
    assert link_info.is_symlink is True
    assert link_info.symlink_target == "t"

    unknown_info = FileInfo.from_fsspec_dict({"name": "p", "type": "fifo"})
    assert unknown_info.type == "other"
    assert unknown_info.is_file is False
    assert unknown_info.is_dir is False

    missing_type_info = FileInfo.from_fsspec_dict({"name": "x"})
    assert missing_type_info.type == "other"


def test_grep_match_is_frozen_slots_kw_only() -> None:
    match = GrepMatch(
        path="src/app.py",
        line_number=42,
        line="return True",
        submatches=((7, 11),),
        context_before=("if ok:",),
        context_after=("",),
    )

    assert str(match) == "src/app.py:42:return True"
    assert not hasattr(match, "__dict__")

    with pytest.raises(FrozenInstanceError):
        match.line_number = 1  # type: ignore[misc]

    with pytest.raises(TypeError):
        GrepMatch("src/app.py", 42, "return True")  # type: ignore[misc]


def test_fsspec_sync_version_matches_lockfile_and_installed() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    lock_data = tomllib.loads((repo_root / "uv.lock").read_text(encoding="utf-8"))
    fsspec_pkgs = [pkg for pkg in lock_data["package"] if pkg.get("name") == "fsspec"]
    assert len(fsspec_pkgs) == 1
    assert fsspec_pkgs[0]["version"] == FSSPEC_SYNC_VERSION
    assert importlib.metadata.version("fsspec") == FSSPEC_SYNC_VERSION


def test_fsspec_sync_note_metadata() -> None:
    note = FsspecSyncNote(
        version=FSSPEC_SYNC_VERSION,
        upstream=("fsspec/utils.py:glob_translate",),
    )
    assert note.version == FSSPEC_SYNC_VERSION
    assert note.upstream == ("fsspec/utils.py:glob_translate",)
    assert note.notes == ""
    assert not hasattr(note, "__dict__")
    with pytest.raises(FrozenInstanceError):
        note.notes = "changed"  # type: ignore[misc]


def test_public_exports_match_all() -> None:
    for symbol in core_fs.__all__:
        assert hasattr(core_fs, symbol)


def test_file_info_is_hashable_and_extra_counts_for_equality() -> None:
    a = FileInfo(path="a", type="file", extra={"k": 1})
    b = FileInfo(path="a", type="file", extra={"k": 2})
    assert hash(a) == hash(b)
    assert a != b
    assert len({a, FileInfo(path="a", type="file", extra={"k": 1})}) == 1


def test_from_fsspec_dict_keeps_unparsable_timestamp_in_extra() -> None:
    info = FileInfo.from_fsspec_dict({"name": "x", "type": "file", "mtime": "yesterday"})
    assert info.mtime is None
    assert info.extra["mtime"] == "yesterday"
