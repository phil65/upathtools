from concurrent.futures import ThreadPoolExecutor
import re

import fsspec.utils as fsspec_utils
import pytest

from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    common_prefix,
    compile_glob,
    glob as glob_mod,
    glob_translate,
    has_magic,
    other_paths,
)


SAMPLE_PATHS = [
    "",
    "/",
    "a",
    "b",
    "ab",
    "a.txt",
    "b.txt",
    ".hidden",
    ".hidden.txt",
    "a/b",
    "a/b.txt",
    "a/c.txt",
    "a/.hidden",
    "a/.hidden/c.txt",
    "a/b/c",
    "a/b/c.txt",
    "a/b/d/e.txt",
    "a/x/y/b",
    "/a/b",
    "/a/b/c.txt",
    "/a/.hidden/c.txt",
    "foo+bar(1).txt",
    "foo[1].txt",
    "a-b.txt",
    "a\\b.txt",
    "dir/",
    "a/b/",
    "a/1",
    "a/9",
    "a/!",
    "a/^",
    "a/]",
    "a/[",
    "a/&",
    "a/~",
    "a/|",
    "a/b\nc.txt",
]


@pytest.mark.parametrize(
    "pattern",
    [
        "*",
        "*.txt",
        "?",
        "a/?",
        "**",
        "a/**",
        "**/b",
        "a/**/b",
        "**/*.txt",
        "a/**/*.txt",
        "/*.txt",
        "/**/*.txt",
        "[abc]",
        "[abc].txt",
        "[!a]",
        "[!a].txt",
        "a/[0-9]",
        "a/[a-z]",
        "a/[z-a]",
        "a/[]",
        "a/[!]",
        "a/[^a]",
        "a/[[]",
        "a/[]a]",
        "a/[!]]",
        "a/[a-]",
        "a/[a-c-e]",
        "a/[&~|]",
        "a/[abc",
        ".*",
        "a/.*",
        "a/.hidden/*.txt",
        "foo+bar(1).txt",
        "foo[[]1].txt",
        "a\\b.txt",
        "a/b*/c.txt",
        "a/*",
        "a/*/",
        "dir/",
        "",
        "/",
    ],
)
def test_glob_translate_matches_fsspec_oracle(pattern: str) -> None:
    expected_regex = fsspec_utils.glob_translate(pattern)
    actual_regex = glob_translate(pattern)

    assert actual_regex == expected_regex

    expected_re = re.compile(expected_regex)
    actual_re = compile_glob(pattern)

    expected_matches = [p for p in SAMPLE_PATHS if expected_re.match(p)]
    actual_matches = [p for p in SAMPLE_PATHS if actual_re.match(p)]
    assert actual_matches == expected_matches


@pytest.mark.parametrize("bad_pattern", ["a**b", "**a", "a**", "dir/a**b/c", "a/***/b"])
def test_glob_translate_rejects_embedded_double_star(bad_pattern: str) -> None:
    with pytest.raises(ValueError, match=r"\*\*"):
        fsspec_utils.glob_translate(bad_pattern)
    with pytest.raises(ValueError, match=r"\*\*"):
        glob_translate(bad_pattern)


def test_compile_glob_caches_and_is_thread_safe() -> None:
    compile_glob.cache_clear()
    pat1 = compile_glob("a/**/*.py")
    pat2 = compile_glob("a/**/*.py")
    assert pat1 is pat2
    assert compile_glob.cache_info().hits >= 1

    patterns = [f"dir_{i}/**/*.txt" for i in range(64)]

    def worker(idx: int) -> re.Pattern[str]:
        return compile_glob(patterns[idx % len(patterns)])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(500)))

    assert all(isinstance(r, re.Pattern) for r in results)


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("*.py", True),
        ("a/b/?.txt", True),
        ("a/[abc].txt", True),
        ("a/**/b", True),
        ("a/b/c.txt", False),
        ("a/b]c.txt", False),
        ("", False),
    ],
)
def test_has_magic(pattern: str, expected: bool) -> None:
    assert has_magic(pattern) is expected


@pytest.mark.parametrize(
    "paths",
    [
        ["/a/b/c", "/a/b/d"],
        ["/a/b"],
        ["a/b", "c/d"],
        ["/a/b", "/c/d"],
        ["a", "a/b"],
        ["a/b/c", "a/b/c"],
    ],
)
def test_common_prefix_matches_fsspec_oracle(paths: list[str]) -> None:
    assert common_prefix(paths) == fsspec_utils.common_prefix(paths)


def test_common_prefix_empty() -> None:
    assert common_prefix([]) == ""


@pytest.mark.parametrize(
    ("paths", "path2", "exists", "flatten"),
    [
        (["/a/b/c", "/a/b/d"], "/x", False, False),
        (["/a/b/c", "/a/b/d"], "/x", True, False),
        (["/a/b/c", "/a/b/d"], "/x/", False, True),
        (["a/b", "c/d"], "/x", False, False),
        (["a/b", "c/d"], "x", False, False),
        (["/a/b", "/c/d"], "/x", False, False),
        (["/a/b"], "/x", False, False),
        (["/a/b"], "/x", True, False),
        (["a/b/c", "a/b/d"], "/x", True, False),
        (["a/b", "a/c"], "/x", True, False),
        (["a/b/c", "a/b/d"], ["x/1", "x/2"], False, False),
    ],
)
def test_other_paths_matches_fsspec_oracle(
    paths: list[str],
    path2: str | list[str],
    exists: bool,
    flatten: bool,
) -> None:
    expected = fsspec_utils.other_paths(paths, path2, exists=exists, flatten=flatten)
    actual = other_paths(paths, path2, exists=exists, flatten=flatten)
    assert actual == expected


def test_other_paths_edge_cases() -> None:
    assert other_paths([], "/dest") == []
    assert other_paths([], []) == []
    with pytest.raises(ValueError, match="does not match"):
        other_paths(["a", "b"], ["x"])


def test_glob_sync_note() -> None:
    assert glob_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION
    assert "fsspec/utils.py:glob_translate" in glob_mod.__fsspec_sync__.upstream
    assert "fsspec/utils.py:other_paths" in glob_mod.__fsspec_sync__.upstream
