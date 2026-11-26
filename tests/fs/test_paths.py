from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import fsspec.core as fsspec_core
import fsspec.utils as fsspec_utils
import pytest

from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    ChainLink,
    PathEscapeError,
    check_contained,
    infer_storage_options,
    join_path,
    normalize_path,
    parent_path,
    paths as paths_mod,
    split_protocol,
    strip_protocol,
    tokenize,
    unchain,
)


@pytest.mark.parametrize(
    "url",
    [
        "s3://bucket/key/file.txt",
        "file:///tmp/a/b.txt",
        "file:///",
        "memory:///a/b",
        "memory://a/b",
        "/tmp/local/path",
        "relative/path.txt",
        "",
        "C:/Users/alice/file.txt",
        r"C:\Users\alice\file.txt",
        "C://Users/alice/file.txt",
        "data:text/plain;base64,SGVsbG8=",
        "://no-scheme",
        "simplecache::s3://bucket/key",
        "http://example.com:8080/a/b?x=1",
        "s3+custom://bucket/key",
        Path("/tmp/from/pathlib"),
    ],
)
def test_split_protocol_matches_fsspec(url: str | Path) -> None:
    result = split_protocol(url)
    assert isinstance(result, tuple)
    assert result == tuple(fsspec_core.split_protocol(url))


@pytest.mark.parametrize(
    ("url", "protocols", "root_marker", "expected"),
    [
        ("s3://bucket/key/file.txt", None, "", "bucket/key/file.txt"),
        ("s3://bucket/key/", None, "", "bucket/key"),
        ("file:///tmp/a/b/", None, "", "/tmp/a/b"),
        ("file:///", None, "/", "/"),
        ("file:///", None, "", ""),
        ("memory:///a/b", "memory", "", "/a/b"),
        ("s3://bucket/key", "gcs", "", "s3://bucket/key"),
        ("s3a://bucket/key", ("s3", "s3a"), "", "bucket/key"),
        ("zip::inner/file.txt", "zip", "", "inner/file.txt"),
        ("data:text/plain,hello", "data", "", "text/plain,hello"),
        ("data:text/plain,hello", None, "", "text/plain,hello"),
        ("C:/Users/alice/", None, "", "C:/Users/alice"),
        ("/", None, "/", "/"),
        ("///", None, "/", "/"),
        (Path("/tmp/a/"), None, "", "/tmp/a"),
    ],
)
def test_strip_protocol(
    url: str | Path,
    protocols: str | tuple[str, ...] | None,
    root_marker: str,
    expected: str,
) -> None:
    assert strip_protocol(url, protocols=protocols, root_marker=root_marker) == expected


@pytest.mark.parametrize(
    ("url", "root_marker"),
    [
        ("file:///tmp/a/b", "/"),
        ("file:///tmp/a/b/", "/"),
        ("file:///", "/"),
        ("/tmp/a/b", "/"),
        ("/tmp/a/b/", "/"),
        ("/", "/"),
        ("memory:///a/b/c", ""),
        ("memory:///a/b/", ""),
        ("memory:///", ""),
        ("zip://x.txt", ""),
        ("zip://dir/x.txt", ""),
    ],
)
def test_strip_protocol_oracle_against_fsspec(url: str, root_marker: str) -> None:
    assert strip_protocol(url, root_marker=root_marker) == fsspec_core.strip_protocol(url)


@pytest.mark.parametrize(
    ("path", "root_marker", "expected"),
    [
        ("/a/b/c", "/", "/a/b"),
        ("/a/b/c/", "/", "/a/b"),
        ("/a", "/", "/"),
        ("/", "/", "/"),
        ("", "/", "/"),
        ("///", "/", "/"),
        ("/a//b", "/", "/a"),
        ("a/b/c", "", "a/b"),
        ("a/b/c/", "", "a/b"),
        ("a", "", ""),
        ("", "", ""),
        ("a/b", "/", "/a"),
        ("///a/b", "/", "/a"),
    ],
)
def test_parent_path(path: str, root_marker: str, expected: str) -> None:
    assert parent_path(path, root_marker=root_marker) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/", "/"),
        ("///", "/"),
        ("/.", "/"),
        ("/./.", "/"),
        ("/a/b/c", "/a/b/c"),
        ("/a/b/c/", "/a/b/c"),
        ("/a//b///c/", "/a/b/c"),
        ("/a/./b/./c", "/a/b/c"),
        ("/a/b/../c", "/a/c"),
        ("/a/..", "/"),
        ("/a/b/../..", "/"),
        ("/a/.hidden/..file/.../x", "/a/.hidden/..file/.../x"),
        ("", ""),
        (".", ""),
        ("./a/b", "a/b"),
        ("a//b/./c/", "a/b/c"),
        ("a/b/../c", "a/c"),
        ("a/..", ""),
        ("a/b/../..", ""),
        ("..a/b", "..a/b"),
    ],
)
def test_normalize_path(raw: str, expected: str) -> None:
    assert normalize_path(raw) == expected


@pytest.mark.parametrize(
    "escaping_path",
    [
        "/..",
        "/../a",
        "/a/../..",
        "/a/b/../../../c",
        "..",
        "../a",
        "a/../..",
        "a/b/../../../c",
    ],
)
def test_normalize_path_raises_on_root_escape(escaping_path: str) -> None:
    with pytest.raises(PathEscapeError):
        normalize_path(escaping_path)


@pytest.mark.parametrize(
    ("parts", "expected"),
    [
        ((), ""),
        (("",), ""),
        (("", ""), ""),
        (("/",), "/"),
        (("/", ""), "/"),
        (("/", "/"), "/"),
        (("/", "a", "b"), "/a/b"),
        (("/a", "b", "c"), "/a/b/c"),
        (("/a/", "b/", "c/"), "/a/b/c"),
        (("/a/", "/b", "/c"), "/a/b/c"),
        (("a", "b", "c"), "a/b/c"),
        (("a/", "/b/", "c/"), "a/b/c"),
        (("a", "", "b"), "a/b"),
        (("", "/a", "b"), "/a/b"),
        (("/a", ""), "/a"),
    ],
)
def test_join_path(parts: tuple[str, ...], expected: str) -> None:
    assert join_path(*parts) == expected


@pytest.mark.parametrize(
    ("url", "kwargs"),
    [
        (
            "simplecache::zip://x.txt::file:///tmp/a.zip",
            {
                "simplecache": {"cache_storage": "/cache"},
                "zip": {"mode": "r"},
                "file": {"auto_mkdir": True},
            },
        ),
        (
            "simplecache::zip://x.txt::file:///tmp/a.zip",
            {"cache_storage": "/cache", "file": {"auto_mkdir": True}},
        ),
        ("simplecache::file:///tmp/a.txt", {}),
        ("filecache::file:///tmp/a.txt", {"filecache": {"same_names": True}}),
        ("blockcache::file:///tmp/a.txt", {}),
        ("zip::file:///tmp/a.zip", {}),
        ("tar::file:///tmp/a.tar", {}),
        ("simplecache::zip::file:///tmp/a.zip", {}),
        ("zip://a/b.txt::memory:///archive.zip", {"zip": {"mode": "r"}}),
        ("/tmp/plain.txt", {"auto_mkdir": True}),
        ("memory:///a/b.txt", {"foo": 1}),
        ("simplecache::s3://bucket/key.txt", {"s3": {"anon": True}}),
        ("/tmp/override.txt", {"protocol": "memory"}),
    ],
)
def test_unchain_matches_fsspec_oracle(url: str, kwargs: dict[str, Any]) -> None:
    # Copy kwargs because fsspec._un_chain mutates nested per-protocol dicts in place.
    fsspec_kw = {k: dict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
    ours_kw = {k: dict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
    snapshot = {k: dict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}

    expected = fsspec_core._un_chain(url, fsspec_kw)
    actual = unchain(url, ours_kw)

    assert ours_kw == snapshot  # caller kwargs not mutated
    assert [(link.path, link.protocol, link.kwargs) for link in actual] == expected


def test_chain_link_is_frozen_with_slots() -> None:
    link = ChainLink("x.txt", "zip", {"mode": "r"})
    assert link.path == "x.txt"
    assert link.protocol == "zip"
    assert link.kwargs == {"mode": "r"}
    assert not hasattr(link, "__dict__")
    with pytest.raises(FrozenInstanceError):
        link.path = "y.txt"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("root", "paths"),
    [
        ("/dest", ["/dest", "/dest/a", "/dest/a/b.txt", "/dest/sub/../a.txt"]),
        ("/", ["/a", "/a/b"]),
        ("rel/dest", ["rel/dest", "rel/dest/file.txt", "rel/dest/sub/../ok.txt"]),
    ],
)
def test_check_contained_accepts_valid_paths(root: str, paths: list[str]) -> None:
    fsspec_utils.check_contained(root, paths)
    check_contained(root, paths)


@pytest.mark.parametrize(
    ("root", "paths"),
    [
        ("/dest", ["/dest/../escape.txt"]),
        ("/dest", ["/dest/a/../../escape.txt"]),
        ("/dest", ["/dest2/file.txt"]),
        ("/dest", ["/other/file.txt"]),
        ("rel/dest", ["rel/dest/../escape.txt"]),
        ("rel/dest", ["rel/dest2/file.txt"]),
    ],
)
def test_check_contained_rejects_escapes_matching_fsspec(root: str, paths: list[str]) -> None:
    with pytest.raises(ValueError, match="outside the destination"):
        fsspec_utils.check_contained(root, paths)
    with pytest.raises(PathEscapeError):
        check_contained(root, paths)


@pytest.mark.parametrize(
    ("urlpath", "inherit"),
    [
        ("/mnt/datasets/test.csv", None),
        ("C:/Users/test/file.csv", None),
        (r"C:\Users\test\file.csv", None),
        ("file:///mnt/datasets/test.csv", None),
        ("file:///C:/Users/test/file.csv", None),
        ("file:///C|/Users/test/file.csv", None),
        ("http://example.com/a/b?q=1#frag", None),
        ("https://example.com/a/b", None),
        ("s3://MyBucket/path/file.csv", None),
        ("s3a://MyBucket/path/file.csv", None),
        ("gcs://MyBucket/path/file.csv", None),
        ("gs://MyBucket/path/file.csv", None),
        (
            "hdfs://username:pwd@node:123/mnt/datasets/test.csv?q=1#frag",
            {"extra": "value"},
        ),
        ("sftp://user@host:22/path/file", {"host": "host", "timeout": 10}),
    ],
)
def test_infer_storage_options_matches_fsspec(
    urlpath: str,
    inherit: dict[str, Any] | None,
) -> None:
    expected = fsspec_utils.infer_storage_options(urlpath, inherit_storage_options=inherit)
    actual = infer_storage_options(urlpath, inherit_storage_options=inherit)
    assert actual == expected


def test_infer_storage_options_collision_raises_key_error() -> None:
    url = "hdfs://user@hostA:8020/path"
    inherit = {"host": "hostB"}
    with pytest.raises(KeyError):
        fsspec_utils.infer_storage_options(url, inherit_storage_options=inherit)
    with pytest.raises(KeyError):
        infer_storage_options(url, inherit_storage_options=inherit)


def test_tokenize_is_deterministic_for_dict_and_kwarg_ordering() -> None:
    md5_hex_len = 32
    tok1 = tokenize({"a": 1, "b": 2}, x=10, y=20)
    tok2 = tokenize({"b": 2, "a": 1}, y=20, x=10)
    assert tok1 == tok2
    assert len(tok1) == md5_hex_len
    assert int(tok1, 16) >= 0

    nested1 = tokenize({"outer": {"b": [1, 2], "a": {"z": 1, "m": 2}}})
    nested2 = tokenize({"outer": {"a": {"m": 2, "z": 1}, "b": [1, 2]}})
    assert nested1 == nested2

    # Mixed key types in mappings do not crash sorting
    mixed1 = tokenize({1: "one", "two": 2})
    mixed2 = tokenize({"two": 2, 1: "one"})
    assert mixed1 == mixed2

    # Sets are deterministic and distinct from lists
    assert tokenize({1, 2, 3}) == tokenize({3, 1, 2})
    assert tokenize({1, 2, 3}) != tokenize([1, 2, 3])

    # Different values produce different tokens
    assert tokenize({"a": 1}) != tokenize({"a": 2})
    assert tokenize("hello") != tokenize("world")


def test_paths_sync_note_and_no_runtime_fsspec_import() -> None:
    assert paths_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION
    assert "fsspec/core.py:_un_chain" in paths_mod.__fsspec_sync__.upstream
    assert "fsspec" not in paths_mod.__dict__
