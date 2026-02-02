"""Helper functions for CLI-style filesystem operations."""

from __future__ import annotations

import difflib
import re
from typing import TYPE_CHECKING, Any, Literal


if TYPE_CHECKING:
    from collections.abc import Sequence

JSONValue = str | int | float | bool | None | list["JSONValue"] | dict[str, "JSONValue"]


def compute_diff(
    content1: str,
    content2: str,
    path1: str,
    path2: str,
    *,
    output_format: Literal["unified", "context"] = "unified",
    context_lines: int = 3,
    brief: bool = False,
    report_identical: bool = False,
    ignore_case: bool = False,
    ignore_whitespace: bool = False,
    ignore_blank_lines: bool = False,
) -> str:
    """Compute diff between two file contents.

    This is a pure function that performs the CPU-intensive diff computation.
    Can be safely run in a thread pool.

    Args:
        content1: Content of the first file
        content2: Content of the second file
        path1: Path to first file (for output headers)
        path2: Path to second file (for output headers)
        output_format: Output format - 'unified' or 'context'
        context_lines: Number of context lines around changes
        brief: Only report whether files differ
        report_identical: Report when files are identical
        ignore_case: Ignore case differences
        ignore_whitespace: Ignore all whitespace
        ignore_blank_lines: Ignore changes in blank lines

    Returns:
        Diff output as string
    """
    lines1 = content1.splitlines(keepends=True)
    lines2 = content2.splitlines(keepends=True)

    # Apply transformations for comparison
    cmp1 = _transform_lines(lines1, ignore_case, ignore_whitespace, ignore_blank_lines)
    cmp2 = _transform_lines(lines2, ignore_case, ignore_whitespace, ignore_blank_lines)

    # Check if identical
    if cmp1 == cmp2:
        if report_identical:
            return f"Files {path1} and {path2} are identical\n"
        return ""

    # Brief mode - just report difference
    if brief:
        return f"Files {path1} and {path2} differ\n"

    # Generate diff output (use original lines for output, not transformed)
    if output_format == "context":
        diff_lines = difflib.context_diff(
            lines1, lines2, fromfile=path1, tofile=path2, n=context_lines
        )
    else:
        diff_lines = difflib.unified_diff(
            lines1, lines2, fromfile=path1, tofile=path2, n=context_lines
        )

    return "".join(diff_lines)


def _transform_lines(
    lines: Sequence[str],
    ignore_case: bool,
    ignore_whitespace: bool,
    ignore_blank_lines: bool,
) -> list[str]:
    """Apply transformations to lines for comparison.

    Args:
        lines: Lines to transform
        ignore_case: Convert to lowercase
        ignore_whitespace: Remove all whitespace
        ignore_blank_lines: Remove blank lines

    Returns:
        Transformed lines
    """
    result: list[str] = list(lines)
    if ignore_case:
        result = [line.lower() for line in result]
    if ignore_whitespace:
        result = [re.sub(r"\s+", "", line) for line in result]
    if ignore_blank_lines:
        result = [line for line in result if line.strip()]
    return result


def apply_jq_filter[T](
    content: str,
    filter: str,
    *,
    return_type: type[T] | None = None,
    args: dict[str, str] | None = None,
    json_args: dict[str, Any] | None = None,
) -> JSONValue | T:
    """Apply a jq filter to JSON content.

    This is a pure function that performs the jq filtering.
    Can be safely run in a thread pool for large documents.

    Args:
        content: JSON content as string
        filter: jq filter expression (e.g., '.foo', '.[] | select(.x > 1)')
        return_type: Expected return type (validated at runtime)
        args: String variables accessible as $name in filter
        json_args: JSON variables accessible as $name in filter

    Returns:
        Filtered result, optionally validated against return_type

    Raises:
        TypeError: If result doesn't match return_type
        ValueError: If jq filter is invalid
    """
    import jq as jq_lib

    # Merge args and json_args
    all_args = {**(args or {}), **(json_args or {})}
    prog = jq_lib.compile(filter, args=all_args if all_args else None)
    results = prog.input_text(content).all()

    # Unwrap single results
    result: JSONValue = results[0] if len(results) == 1 else results

    if return_type is not None and not isinstance(result, return_type):
        msg = f"Expected {return_type.__name__}, got {type(result).__name__}"
        raise TypeError(msg)

    return result
