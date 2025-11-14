"""Skills-aware filesystem that enriches directory listings with SKILL.md metadata."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem
import yaml

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    from upath.types import JoinablePathLike

logger = logging.getLogger(__name__)


class SkillsPath(BaseUPath):
    """UPath implementation for Skills filesystem."""

    __slots__ = ()


class SkillsFileSystem(BaseAsyncFileSystem[SkillsPath]):
    """Filesystem wrapper that enriches directory listings with skill metadata."""

    protocol = "skills"
    root_marker = "/"
    upath_cls = SkillsPath

    def __init__(
        self,
        wrapped_fs: AbstractFileSystem | JoinablePathLike,
        **storage_options,
    ):
        """Initialize skills filesystem.

        Args:
            wrapped_fs: Filesystem to wrap or path to create filesystem from
            **storage_options: Additional options passed to wrapped filesystem
        """
        super().__init__(**storage_options)

        # Handle wrapped filesystem
        if isinstance(wrapped_fs, AbstractFileSystem):
            self.wrapped_fs = wrapped_fs
        else:
            from upathtools.helpers import upath_to_fs

            self.wrapped_fs = upath_to_fs(wrapped_fs, asynchronous=True)

        logger.debug(
            "Created SkillsFileSystem wrapping %s", type(self.wrapped_fs).__name__
        )

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse skills URL format: skills://wrapped_protocol://path."""
        if not path.startswith("skills://"):
            return {}

        # Extract wrapped filesystem URL
        wrapped_url = path[9:]  # Remove "skills://"
        return {"wrapped_fs": wrapped_url}

    async def _is_skill_directory(self, path: str) -> bool:
        """Check if directory contains SKILL.md file."""
        try:
            skill_path = self._join_path(path, "SKILL.md")
            if isinstance(self.wrapped_fs, AsyncFileSystem):
                return await self.wrapped_fs._exists(skill_path)
            return self.wrapped_fs.exists(skill_path)
        except Exception:  # noqa: BLE001
            return False

    def _join_path(self, *parts: str) -> str:
        """Join path parts using wrapped filesystem's separator."""
        return self.wrapped_fs.sep.join(
            str(p).strip(self.wrapped_fs.sep) for p in parts if p
        )

    async def _parse_skill_metadata(self, path: str) -> dict[str, Any] | None:
        """Parse SKILL.md metadata from directory."""
        try:
            skill_path = self._join_path(path, "SKILL.md")

            # Read SKILL.md content
            if isinstance(self.wrapped_fs, AsyncFileSystem):
                content = await self.wrapped_fs._cat_file(skill_path)
            else:
                content = self.wrapped_fs.cat_file(skill_path)

            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Parse YAML frontmatter
            if content.startswith("---\n"):
                try:
                    # Split frontmatter and content
                    parts = content.split("---\n", 2)
                    if len(parts) >= 2:  # noqa: PLR2004
                        frontmatter = parts[1].strip()
                        metadata = yaml.safe_load(frontmatter) or {}

                        # Add skill-specific fields
                        skill_info = {
                            "is_skill": True,
                            "skill_name": metadata.get("name", ""),
                            "skill_description": metadata.get("description", ""),
                            "skill_metadata": metadata,
                        }

                        logger.debug(
                            "Parsed skill metadata for %s: %s", path, metadata.get("name")
                        )
                        return skill_info

                except yaml.YAMLError as e:
                    logger.warning(
                        "Failed to parse YAML frontmatter in %s: %s", skill_path, e
                    )

        except Exception as e:  # noqa: BLE001
            logger.debug("Could not parse skill metadata for %s: %s", path, e)

        return None

    async def _enhance_with_skill_info(self, info: dict[str, Any]) -> dict[str, Any]:
        """Enhance file info with skill metadata if it's a skill directory."""
        enhanced_info = info.copy()

        # Only check directories
        if info.get("type") == "directory":
            path = info["name"]

            # Check if it's a skill directory
            if await self._is_skill_directory(path):
                skill_metadata = await self._parse_skill_metadata(path)
                if skill_metadata:
                    enhanced_info.update(skill_metadata)

        return enhanced_info

    async def _cat_file(self, path: str, start=None, end=None, **kwargs):
        """Read file contents."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            return await self.wrapped_fs._cat_file(path, start=start, end=end, **kwargs)
        return self.wrapped_fs.cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value, **kwargs):
        """Write file contents."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            await self.wrapped_fs._pipe_file(path, value, **kwargs)
        else:
            self.wrapped_fs.pipe_file(path, value, **kwargs)

    async def _info(self, path: str, **kwargs):
        """Get enhanced info about a path."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            info = await self.wrapped_fs._info(path, **kwargs)
        else:
            info = self.wrapped_fs.info(path, **kwargs)

        return await self._enhance_with_skill_info(info)

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(self, path: str, detail=True, **kwargs):
        """List directory contents with skill metadata enhancement."""
        # Get base listing from wrapped filesystem
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            entries = await self.wrapped_fs._ls(path, detail=True, **kwargs)
        else:
            entries = self.wrapped_fs.ls(path, detail=True, **kwargs)

        if not detail:
            return [entry["name"] for entry in entries]

        # Enhance entries with skill metadata (in parallel for performance)
        enhanced_entries = await asyncio.gather(
            *[self._enhance_with_skill_info(entry) for entry in entries],
            return_exceptions=True,
        )

        # Filter out exceptions and log them
        result = []
        for i, entry in enumerate(enhanced_entries):
            if isinstance(entry, Exception):
                logger.warning(
                    "Failed to enhance entry %s: %s", entries[i].get("name"), entry
                )
                result.append(entries[i])  # Use original entry
            else:
                result.append(entry)

        return result

    async def _exists(self, path: str, **kwargs):
        """Check if path exists."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            return await self.wrapped_fs._exists(path, **kwargs)
        return self.wrapped_fs.exists(path, **kwargs)

    async def _isdir(self, path: str):
        """Check if path is a directory."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            return await self.wrapped_fs._isdir(path)
        return self.wrapped_fs.isdir(path)

    async def _isfile(self, path: str):
        """Check if path is a file."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            return await self.wrapped_fs._isfile(path)
        return self.wrapped_fs.isfile(path)

    async def _makedirs(self, path: str, exist_ok=False):
        """Create directories."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            await self.wrapped_fs._makedirs(path, exist_ok=exist_ok)
        else:
            self.wrapped_fs.makedirs(path, exist_ok=exist_ok)

    async def _rm_file(self, path: str):
        """Remove a file."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            await self.wrapped_fs._rm_file(path)
        else:
            self.wrapped_fs.rm_file(path)

    async def _rm(self, path: str, recursive=False, maxdepth=None):
        """Remove file or directory."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            await self.wrapped_fs._rm(path, recursive=recursive, maxdepth=maxdepth)
        else:
            self.wrapped_fs.rm(path, recursive=recursive, maxdepth=maxdepth)

    async def _cp_file(self, path1: str, path2: str, **kwargs):
        """Copy a file."""
        if isinstance(self.wrapped_fs, AsyncFileSystem):
            await self.wrapped_fs._cp_file(path1, path2, **kwargs)
        else:
            self.wrapped_fs.cp_file(path1, path2, **kwargs)

    async def list_skills(self, path: str = "/") -> list[dict[str, Any]]:
        """Get all skill directories under a path.

        Args:
            path: Path to search for skills

        Returns:
            List of skill information dictionaries
        """
        skills = []

        try:
            entries = await self._ls(path, detail=True)

            for entry in entries:
                if entry.get("is_skill"):
                    skills.append({
                        "path": entry["name"],
                        "name": entry.get("skill_name", ""),
                        "description": entry.get("skill_description", ""),
                        "metadata": entry.get("skill_metadata", {}),
                    })
                elif entry.get("type") == "directory":
                    # Recursively search subdirectories
                    subskills = await self.list_skills(entry["name"])
                    skills.extend(subskills)

        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to list skills in %s: %s", path, e)

        return skills


if __name__ == "__main__":
    # Example usage
    fs = SkillsFileSystem("file:///home/phil65/dev/oss/upathtools/.claude/skills/")

    async def main():
        skills = await fs.list_skills()
        print(skills)

    asyncio.run(main())
