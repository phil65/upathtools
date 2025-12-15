"""Skills-aware filesystem using WrapperFileSystem with info callback."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import yaml

from upathtools import is_directory
from upathtools.filesystems.base.wrapper import WrapperFileSystem


if TYPE_CHECKING:
    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem
    from upath.types import JoinablePathLike


logger = logging.getLogger(__name__)


async def _skill_info_callback(  # noqa: PLR0911
    info: dict[str, Any],
    fs: WrapperFileSystem,
) -> dict[str, Any]:
    """Enrich directory info with skill metadata if it contains SKILL.md."""
    if not await is_directory(fs, info["name"], entry_type=info.get("type")):
        return info

    path = info["name"]
    skill_path = f"{path.rstrip('/')}/SKILL.md"

    try:
        if not await fs.fs._exists(skill_path):
            return info
    except Exception:  # noqa: BLE001
        return info

    try:
        content = await fs.fs._cat_file(skill_path)
        if isinstance(content, bytes):
            content = content.decode("utf-8")

        if not content.startswith("---\n"):
            return info

        parts = content.split("---\n", 2)
        if len(parts) < 2:  # noqa: PLR2004
            return info

        frontmatter = parts[1].strip()
        metadata = yaml.safe_load(frontmatter) or {}

        logger.debug("Parsed skill metadata for %s: %s", path, metadata.get("name"))

        return {
            **info,
            "is_skill": True,
            "skill_name": metadata.get("name", ""),
            "skill_description": metadata.get("description", ""),
            "skill_version": metadata.get("version", ""),
            "skill_author": metadata.get("author", ""),
            "skill_tags": metadata.get("tags", []),
            "skill_dependencies": metadata.get("dependencies", []),
            "skill_metadata": metadata,
        }

    except yaml.YAMLError as e:
        logger.warning("Failed to parse YAML frontmatter in %s: %s", skill_path, e)
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not parse skill metadata for %s: %s", path, e)

    return info


def create_skills_filesystem(
    wrapped_fs: AbstractFileSystem | AsyncFileSystem | JoinablePathLike,
    **storage_options: Any,
) -> WrapperFileSystem:
    """Create a filesystem that enriches directories with skill metadata.

    Args:
        wrapped_fs: Filesystem to wrap, or path to create filesystem from
        **storage_options: Additional options passed to wrapped filesystem

    Returns:
        WrapperFileSystem with skill metadata enrichment
    """
    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from upathtools.helpers import upath_to_fs

    if isinstance(wrapped_fs, AsyncFileSystem | AbstractFileSystem):
        fs = wrapped_fs
    else:
        fs = upath_to_fs(wrapped_fs, **storage_options)

    return WrapperFileSystem(fs=fs, info_callback=_skill_info_callback)


async def list_skills(fs: WrapperFileSystem, path: str = "/") -> list[dict[str, Any]]:
    """Get all skill directories under a path.

    Args:
        fs: A WrapperFileSystem (ideally created with create_skills_filesystem)
        path: Path to search for skills

    Returns:
        List of skill info dicts with path, name, description, and metadata
    """
    skills = []

    try:
        entries = await fs._ls(path, detail=True)

        for entry in entries:
            if entry.get("is_skill"):
                skills.append({
                    "path": entry["name"],
                    "name": entry.get("skill_name", ""),
                    "description": entry.get("skill_description", ""),
                    "metadata": entry.get("skill_metadata", {}),
                })
            elif await is_directory(fs, entry["name"], entry_type=entry.get("type")):
                subskills = await list_skills(fs, entry["name"])
                skills.extend(subskills)

    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to list skills in %s: %s", path, e)

    return skills


if __name__ == "__main__":
    import asyncio

    fs = create_skills_filesystem("file:///home/phil65/dev/oss/upathtools/.claude/skills/")

    async def main() -> None:
        skills = await list_skills(fs)
        print(skills)

    asyncio.run(main())
