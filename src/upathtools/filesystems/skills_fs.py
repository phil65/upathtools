"""Skills-aware filesystem that enriches directory listings with SKILL.md metadata."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.spec import AbstractFileSystem
import yaml

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo
from upathtools.helpers import upath_to_fs


if TYPE_CHECKING:
    from upath.types import JoinablePathLike


class SkillsInfo(FileInfo, total=False):
    """Info dict for Skills filesystem paths."""

    size: int
    skill_name: str
    skill_description: str
    skill_version: str
    skill_author: str
    skill_tags: list[str]
    skill_dependencies: list[str]


logger = logging.getLogger(__name__)


class SkillsPath(BaseUPath[SkillsInfo]):
    """UPath implementation for Skills filesystem."""

    __slots__ = ()


class SkillsFileSystem(BaseAsyncFileSystem[SkillsPath, SkillsInfo]):
    """Filesystem wrapper that enriches directory listings with skill metadata."""

    protocol = "skills"
    root_marker = "/"
    upath_cls = SkillsPath

    def __init__(
        self,
        wrapped_fs: AbstractFileSystem | AsyncFileSystem | JoinablePathLike,
        **storage_options,
    ) -> None:
        """Initialize skills filesystem.

        Args:
            wrapped_fs: Filesystem to wrap or path to create filesystem from
            **storage_options: Additional options passed to wrapped filesystem
        """
        super().__init__(**storage_options)

        if isinstance(wrapped_fs, AsyncFileSystem):
            self._fs = wrapped_fs
        else:
            if not isinstance(wrapped_fs, AbstractFileSystem):
                wrapped_fs = upath_to_fs(wrapped_fs)
            self._fs = AsyncFileSystemWrapper(wrapped_fs, asynchronous=True)

        logger.debug("Created SkillsFileSystem wrapping %s", type(self._fs).__name__)

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse skills URL format: skills://wrapped_protocol://path."""
        if not path.startswith("skills://"):
            return {}
        return {"wrapped_fs": path[9:]}

    def _join_path(self, *parts: str) -> str:
        """Join path parts using wrapped filesystem's separator."""
        return self._fs.sep.join(str(p).strip(self._fs.sep) for p in parts if p)

    async def _is_skill_directory(self, path: str) -> bool:
        """Check if directory contains SKILL.md file."""
        try:
            skill_path = self._join_path(path, "SKILL.md")
            return await self._fs._exists(skill_path)
        except Exception:  # noqa: BLE001
            return False

    async def _parse_skill_metadata(self, path: str) -> dict[str, Any] | None:
        """Parse SKILL.md metadata from directory."""
        try:
            skill_path = self._join_path(path, "SKILL.md")
            content = await self._fs._cat_file(skill_path)

            if isinstance(content, bytes):
                content = content.decode("utf-8")

            if content.startswith("---\n"):
                parts = content.split("---\n", 2)
                if len(parts) >= 2:  # noqa: PLR2004
                    frontmatter = parts[1].strip()
                    metadata = yaml.safe_load(frontmatter) or {}
                    logger.debug("Parsed skill metadata for %s: %s", path, metadata.get("name"))
                    return {
                        "is_skill": True,
                        "skill_name": metadata.get("name", ""),
                        "skill_description": metadata.get("description", ""),
                        "skill_metadata": metadata,
                    }
        except yaml.YAMLError as e:
            logger.warning("Failed to parse YAML frontmatter in %s: %s", path, e)
        except Exception as e:  # noqa: BLE001
            logger.debug("Could not parse skill metadata for %s: %s", path, e)
        return None

    async def _enhance_with_skill_info(self, info: dict[str, Any]) -> SkillsInfo:
        """Enhance file info with skill metadata if it's a skill directory."""
        enhanced_info = SkillsInfo(
            name=info.get("name", ""),
            type=info.get("type", "file"),
            size=info.get("size", 0),
        )

        if info.get("type") == "directory":
            path = info["name"]
            if await self._is_skill_directory(path):
                skill_metadata = await self._parse_skill_metadata(path)
                if skill_metadata:
                    for key in (
                        "skill_name",
                        "skill_description",
                        "skill_version",
                        "skill_author",
                        "skill_tags",
                        "skill_dependencies",
                    ):
                        if key in skill_metadata:
                            enhanced_info[key] = skill_metadata[key]  # type: ignore[literal-required]

        return enhanced_info

    async def _cat_file(self, path: str, start=None, end=None, **kwargs: Any):
        """Read file contents."""
        return await self._fs._cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value, **kwargs: Any) -> None:
        """Write file contents."""
        await self._fs._pipe_file(path, value, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> SkillsInfo:
        """Get enhanced info about a path."""
        info = await self._fs._info(path, **kwargs)
        return await self._enhance_with_skill_info(info)

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[SkillsInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(self, path: str, detail=True, **kwargs: Any) -> list[SkillsInfo] | list[str]:
        """List directory contents with skill metadata enhancement."""
        entries = await self._fs._ls(path, detail=True, **kwargs)

        if not detail:
            return [entry["name"] for entry in entries]

        enhanced_entries = await asyncio.gather(
            *[self._enhance_with_skill_info(entry) for entry in entries],
            return_exceptions=True,
        )

        result = []
        for i, entry in enumerate(enhanced_entries):
            if isinstance(entry, Exception):
                logger.warning("Failed to enhance entry %s: %s", entries[i].get("name"), entry)
                original = entries[i]
                result.append(
                    SkillsInfo(
                        name=original.get("name", ""),
                        type=original.get("type", "file"),
                        size=original.get("size", 0),
                    )
                )
            else:
                result.append(entry)

        return result

    async def _exists(self, path: str, **kwargs: Any):
        """Check if path exists."""
        return await self._fs._exists(path, **kwargs)

    async def _isdir(self, path: str):
        """Check if path is a directory."""
        return await self._fs._isdir(path)

    async def _isfile(self, path: str) -> bool:
        """Check if path is a file."""
        return await self._fs._isfile(path)

    async def _makedirs(self, path: str, exist_ok=False) -> None:
        """Create directories."""
        await self._fs._makedirs(path, exist_ok=exist_ok)

    async def _rm_file(self, path: str) -> None:
        """Remove a file."""
        await self._fs._rm_file(path)

    async def _rm(self, path: str, recursive: bool = False, maxdepth: int | None = None) -> None:
        """Remove file or directory."""
        await self._fs._rm(path, recursive=recursive, maxdepth=maxdepth)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file."""
        await self._fs._cp_file(path1, path2, **kwargs)

    async def list_skills(self, path: str = "/") -> list[dict[str, Any]]:
        """Get all skill directories under a path.

        Args:
            path: Path to search for skills
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
                    subskills = await self.list_skills(entry["name"])
                    skills.extend(subskills)

        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to list skills in %s: %s", path, e)

        return skills


if __name__ == "__main__":
    fs = SkillsFileSystem("file:///home/phil65/dev/oss/upathtools/.claude/skills/")

    async def main() -> None:
        skills = await fs.list_skills()
        print(skills)

    asyncio.run(main())
