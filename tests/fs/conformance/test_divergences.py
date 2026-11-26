"""Verification gate ensuring ``DIVERGENCES.md`` stays synced with real tests."""

import importlib
import inspect
from pathlib import Path
import re


_CONFORMANCE_DIR = Path(__file__).resolve().parent
_DIVERGENCES_MD = _CONFORMANCE_DIR / "DIVERGENCES.md"
_REPO_ROOT = _CONFORMANCE_DIR.parents[2]

_TEST_ID_RE = re.compile(r"`([^`]+::[^`]+)`")
_EXPECTED_COLUMNS = 4
_CLASS_NODE_PARTS = 3


def _parse_divergence_table(md_path: Path) -> list[tuple[str, str, str, str]]:
    """Parse the 4-column markdown table from ``DIVERGENCES.md``."""
    lines = md_path.read_text(encoding="utf-8").splitlines()
    table_lines = [line.strip() for line in lines if line.strip().startswith("|")]
    # Header row + separator row + at least one data row.
    min_table_lines = 3
    assert len(table_lines) >= min_table_lines, (
        f"Expected markdown table in {md_path}, found {len(table_lines)} table lines"
    )

    data_rows: list[tuple[str, str, str, str]] = []
    for raw_row in table_lines[2:]:
        # Split on unescaped '|' while preserving escaped '\|' inside cells.
        cells = [cell.replace(r"\|", "|").strip() for cell in re.split(r"(?<!\\)\|", raw_row)[1:-1]]
        assert len(cells) == _EXPECTED_COLUMNS, (
            f"Expected {_EXPECTED_COLUMNS} columns in DIVERGENCES.md row, "
            f"got {len(cells)}: {raw_row!r}"
        )
        data_rows.append((cells[0], cells[1], cells[2], cells[3]))
    return data_rows


def _resolve_test_node_id(node_id: str) -> object:
    """Resolve a pytest node id ``path/to/test.py::[Class::]test_fn`` to its callable."""
    parts = node_id.split("::")
    min_parts = 2
    assert len(parts) >= min_parts, f"Malformed test node id (missing '::'): {node_id!r}"

    rel_file = parts[0]
    file_path = _REPO_ROOT / rel_file
    assert file_path.is_file(), f"Test file referenced by {node_id!r} does not exist: {file_path}"
    assert rel_file.endswith(".py"), f"Referenced test path must be a .py file: {node_id!r}"

    module_name = rel_file.removesuffix(".py").replace("/", ".")
    obj: object = importlib.import_module(module_name)
    for attr in parts[1:]:
        assert hasattr(obj, attr), f"Could not resolve {attr!r} in {node_id!r} on {obj!r}"
        obj = getattr(obj, attr)

    assert callable(obj), f"Resolved target for {node_id!r} is not callable: {obj!r}"
    assert parts[-1].startswith("test_"), (
        f"Referenced target must be a pytest test function/method: {node_id!r}"
    )
    if len(parts) >= _CLASS_NODE_PARTS:
        parent_cls_name = parts[-2]
        assert parent_cls_name.startswith("Test"), (
            f"Referenced test class must start with 'Test': {node_id!r}"
        )
        parent_cls = getattr(importlib.import_module(module_name), parent_cls_name)
        assert not inspect.isabstract(parent_cls), (
            f"Referenced test class {parent_cls_name!r} is abstract and won't be collected"
        )
    return obj


def test_divergences_matrix_exists_and_all_referenced_tests_resolve() -> None:
    """Every row in ``DIVERGENCES.md`` has content and pins real pytest test node IDs."""
    assert _DIVERGENCES_MD.is_file(), f"Missing divergence matrix: {_DIVERGENCES_MD}"
    rows = _parse_divergence_table(_DIVERGENCES_MD)
    assert rows, "DIVERGENCES.md table must contain at least one row"

    seen_ids: set[str] = set()
    for idx, (fsspec_beh, core_beh, why, tests_cell) in enumerate(rows, start=1):
        assert fsspec_beh, f"Row {idx} has empty 'fsspec behaviour' column"
        assert core_beh, f"Row {idx} has empty 'upathtools.fs behaviour' column"
        assert why, f"Row {idx} has empty 'why' column"

        test_ids = _TEST_ID_RE.findall(tests_cell)
        assert test_ids, (
            f"Row {idx} must list at least one backtick-quoted pytest test id in column 4: "
            f"{tests_cell!r}"
        )
        for node_id in test_ids:
            _resolve_test_node_id(node_id)
            seen_ids.add(node_id)

    # Every strict xfail in tests/fs/test_from_fsspec.py must be documented in DIVERGENCES.md.
    from_fsspec_mod = importlib.import_module("tests.fs.test_from_fsspec")
    for cls_name, cls_obj in inspect.getmembers(from_fsspec_mod, inspect.isclass):
        if not cls_name.startswith("Test") or cls_obj.__module__ != from_fsspec_mod.__name__:
            continue
        for method_name, method_obj in cls_obj.__dict__.items():
            if not method_name.startswith("test_"):
                continue
            marks = getattr(method_obj, "pytestmark", [])
            if any(getattr(m, "name", None) == "xfail" for m in marks):
                expected_id = f"tests/fs/test_from_fsspec.py::{cls_name}::{method_name}"
                assert expected_id in seen_ids, (
                    f"xfail test {expected_id!r} in test_from_fsspec.py is not documented in "
                    f"{_DIVERGENCES_MD.name}"
                )
