"""CLI command parser and executor for filesystem operations.

This module provides a shell-like interface for executing commands on filesystems and paths.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
import shlex
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from upath import UPath

SHORT_TO_LONG = {
    "r": "recursive",
    "i": "ignore_case",
    "v": "invert_match",
    "w": "whole_word",
    "F": "fixed_string",
    "m": "max_count",
    "B": "context_before",
    "A": "context_after",
    "n": "n",
    "a": "all_",
    "l": "long",
    "h": "human_readable",
    "s": "summarize",
    "d": "max_depth",
    "f": "force",
    "p": "parents",
}


@dataclass
class CLIResult[TData]:
    """Result from CLI command execution."""

    data: TData
    command: str

    def __str__(self) -> str:
        """Format result as string."""
        if isinstance(self.data, list):
            return "\n".join(str(item) for item in self.data)
        return str(self.data)

    def __iter__(self):
        """Allow iteration over results."""
        if isinstance(self.data, Sequence):
            yield from self.data
        else:
            yield self.data


async def execute_cli_async(command: str, base: UPath) -> CLIResult:  # noqa: PLR0911
    """Execute a CLI-style command on a filesystem/path asynchronously.

    Args:
        command: Shell-like command string (e.g., "grep pattern file.txt -i")
        base: Base UPath to execute command relative to

    Returns:
        CLIResult with command output

    Examples:
        >>> path = UPath(".")
        >>> result = await execute_cli_async("grep TODO *.py -r", path)
        >>> for match in result:
        ...     print(match)
    """
    from upathtools.cli_ops import acat, adiff, adu, afind, agrep, ahead, als, atail, awc

    parts = shlex.split(command)
    if not parts:
        msg = "Empty command"
        raise ValueError(msg)

    cmd = parts[0]
    args, kwargs = _parse_args(parts[1:])

    match cmd:
        case "grep":
            if len(args) < 1:
                msg = "grep requires pattern argument"
                raise ValueError(msg)

            pattern = args[0]
            path = args[1] if len(args) > 1 else "."
            results = [result async for result in agrep(pattern, path, base, **kwargs)]
            return CLIResult(results, f"grep {pattern} {path}")
        case "find":
            path = args[0] if args else "."

            # Map common find args
            if "name" in args:
                idx = args.index("name")
                if idx + 1 < len(args):
                    kwargs["name"] = args[idx + 1]

            results = [i async for i in afind(path, base, **kwargs)]
            return CLIResult(results, f"find {path}")
        case "head":
            if not args:
                msg = "head requires file argument"
                raise ValueError(msg)

            path = args[0]
            result = await ahead(path, base, **kwargs)
            return CLIResult(result, f"head {path}")
        case "tail":
            if not args:
                msg = "tail requires file argument"
                raise ValueError(msg)

            path = args[0]
            result = await atail(path, base, **kwargs)
            return CLIResult(result, f"tail {path}")

        case "cat":
            if not args:
                msg = "cat requires file argument(s)"
                raise ValueError(msg)

            result = await acat(*args, base=base, **kwargs)
            return CLIResult(result, f"cat {' '.join(args)}")

        case "wc":
            if not args:
                msg = "wc requires file argument"
                raise ValueError(msg)

            path = args[0]
            result = await awc(path, base, **kwargs)
            return CLIResult(result, f"wc {path}")

        case "ls":
            path = args[0] if args else "."
            ls_results = await als(path, base, **kwargs)
            return CLIResult(ls_results, f"ls {path}")
        case "du":
            path = args[0] if args else "."
            du_results = await adu(path, base, **kwargs)
            return CLIResult(du_results, f"du {path}")
        case "diff":
            if len(args) < 2:  # noqa: PLR2004
                msg = "diff requires two file arguments"
                raise ValueError(msg)

            result = await adiff(args[0], args[1], base, **kwargs)
            return CLIResult(result, f"diff {args[0]} {args[1]}")
        case _:
            msg = f"Unknown command: {cmd}"
            raise ValueError(msg)


def execute_cli(command: str, base: UPath) -> CLIResult:
    """Execute a CLI-style command on a filesystem/path (sync wrapper)."""
    return asyncio.run(execute_cli_async(command, base))


def _parse_args(args: list[str]) -> tuple[list[str], dict[str, Any]]:
    """Parse positional args and flags from command arguments.

    Args:
        args: List of argument strings

    Returns:
        Tuple of (positional_args, kwargs_dict)
    """
    positional: list[str] = []
    kwargs: dict[str, Any] = {}
    i = 0

    while i < len(args):
        arg = args[i]

        if arg.startswith("--"):
            # Long option
            key = arg[2:].replace("-", "_")
            if "=" in key:
                k, v = key.split("=", 1)
                kwargs[k] = _parse_value(v)
            elif i + 1 < len(args) and not args[i + 1].startswith("-"):
                kwargs[key] = _parse_value(args[i + 1])
                i += 1
            else:
                kwargs[key] = True
        elif arg.startswith("-") and len(arg) > 1:
            # Short options
            for char in arg[1:]:
                flag_name = SHORT_TO_LONG.get(char, char)
                # Check if next arg is a value for this flag
                if char in "nBAmdc" and i + 1 < len(args) and not args[i + 1].startswith("-"):
                    kwargs[flag_name] = _parse_value(args[i + 1])
                    i += 1
                else:
                    kwargs[flag_name] = True
        else:
            positional.append(arg)

        i += 1

    return positional, kwargs


def _parse_value(value: str) -> Any:
    """Parse a string value to appropriate type."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value
