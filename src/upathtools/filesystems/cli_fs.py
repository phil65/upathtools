"""Filesystem implementation for executing CLI commands."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import shutil
import subprocess
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from fsspec.spec import AbstractFileSystem
from upath import UPath
from upath.types import UNSET_DEFAULT


if TYPE_CHECKING:
    from upath.types import WritablePathLike


logger = logging.getLogger(__name__)


class CliPath(UPath):
    """UPath implementation for CLI filesystems."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    def rename(
        self,
        target: WritablePathLike,
        *,  # note: non-standard compared to pathlib
        recursive: bool = UNSET_DEFAULT,
        maxdepth: int | None = UNSET_DEFAULT,
        **kwargs: Any,
    ) -> Self:
        """Rename operation is not supported."""
        msg = "CliPath does not support rename operations"
        raise NotImplementedError(msg)


class CliFS(AbstractFileSystem):
    """Filesystem for executing CLI commands and capturing their output."""

    protocol = "cli"

    def __init__(self, shell: bool = False, encoding: str = "utf-8", **kwargs: Any):
        """Initialize the CLI filesystem.

        Args:
            shell: Whether to use shell mode for command execution
            encoding: Output encoding
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self.shell = shell
        self.encoding = encoding
        self._available_commands: dict[str, str] | None = None

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return CliPath(path)

    def _get_available_commands(self) -> dict[str, str]:
        """Get mapping of available commands to their full paths."""
        if self._available_commands is not None:
            return self._available_commands

        commands: dict[str, str] = {}
        # Get all directories in PATH
        paths = os.environ.get("PATH", "").split(os.pathsep)

        for dir_path in paths:
            try:
                path = Path(dir_path)
                if not path.is_dir():
                    continue

                # Look for executables with common extensions
                for ext in ["", ".exe", ".cmd", ".bat", ".ps1"]:
                    for file in path.glob(f"*{ext}"):
                        if file.is_file() and os.access(file, os.X_OK):
                            name = file.stem.lower()  # normalize to lowercase
                            if name not in commands:  # first occurrence wins
                                commands[name] = str(file)

            except (OSError, PermissionError):
                continue

        self._available_commands = commands
        return commands

        # Find executables in PATH
        for cmd in ["git", "python", "pip"]:  # Add common commands here
            path = shutil.which(cmd)
            if path:
                commands[cmd] = path

        self._available_commands = commands
        return commands

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    def ls(
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        """List available commands.

        Only lists root-level commands for now.

        Args:
            path: Path to list (must be empty)
            detail: Whether to return detailed information
            **kwargs: Additional options

        Returns:
            List of command names or command details

        Raises:
            NotImplementedError: If path is not empty (subcommands not supported)
        """
        path = self._strip_protocol(path).strip("/")  # type: ignore
        if path:
            msg = "Listing subcommands is not supported"
            raise NotImplementedError(msg)

        commands = self._get_available_commands()

        if not detail:
            return list(commands)

        return [
            {"name": name, "type": "command", "size": 0, "executable": path}
            for name, path in commands.items()
        ]

    def cat(self, path: str) -> bytes:
        """Execute command and return its output.

        Args:
            path: Command to execute

        Returns:
            Command output as bytes

        Raises:
            ValueError: If path/command is empty
            subprocess.CalledProcessError: If command execution fails
        """
        command = self._strip_protocol(path).strip("/")  # type: ignore
        if not command:
            msg = "No command specified"
            raise ValueError(msg)

        try:
            if self.shell:
                # Shell mode - execute as single string
                result = subprocess.check_output(
                    command,
                    shell=True,
                    text=True,
                    encoding=self.encoding,
                )
            else:
                # Split command into args
                args = command.split()
                result = subprocess.check_output(
                    args,
                    text=True,
                    encoding=self.encoding,
                )
            return result.encode(self.encoding)

        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(
                e.returncode, e.cmd, e.output, e.stderr
            ) from None

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get information about a command.

        Args:
            path: Command path
            **kwargs: Additional options

        Returns:
            Command information

        Raises:
            FileNotFoundError: If command doesn't exist
        """
        command = self._strip_protocol(path).strip("/")  # type: ignore
        if not command:
            return {"name": "", "type": "directory", "size": 0}

        # Get just the command name without args
        cmd_name = command.split()[0]
        commands = self._get_available_commands()

        if cmd_name not in commands:
            msg = f"Command not found: {cmd_name}"
            raise FileNotFoundError(msg)

        return {
            "name": cmd_name,
            "type": "command",
            "size": 0,
            "executable": commands[cmd_name],
        }


if __name__ == "__main__":
    fs = CliFS()

    # List available commands
    print("\nAvailable commands:")
    for cmd in fs.ls(detail=True):
        print(f"- {cmd['name']}: {cmd['executable']}")

    # Execute a command
    output = fs.cat("git --version")
    print(f"\nCommand output:\n{output.decode()}")
