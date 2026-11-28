"""Sandbox filesystems for remote execution environments."""

from typing import TYPE_CHECKING

from upathtools._lazy import lazy_exports


if TYPE_CHECKING:
    from upathtools.filesystems.sandbox_filesystems.beam_fs import BeamFS, BeamInfo, BeamPath
    from upathtools.filesystems.sandbox_filesystems.codex_fs import CodexFS, CodexInfo, CodexPath
    from upathtools.filesystems.sandbox_filesystems.cloudflare_fs import (
        CloudflareFS,
        CloudflareInfo,
        CloudflarePath,
    )
    from upathtools.filesystems.sandbox_filesystems.daytona_fs import (
        DaytonaFS,
        DaytonaInfo,
        DaytonaPath,
    )
    from upathtools.filesystems.sandbox_filesystems.e2b_fs import E2BFS, E2BInfo, E2BPath
    from upathtools.filesystems.sandbox_filesystems.hopx_fs import HopXFS, HopXInfo, HopXPath
    from upathtools.filesystems.sandbox_filesystems.microsandbox_fs import (
        MicrosandboxFS,
        MicrosandboxInfo,
        MicrosandboxPath,
    )
    from upathtools.filesystems.sandbox_filesystems.modal_fs import ModalFS, ModalInfo, ModalPath
    from upathtools.filesystems.sandbox_filesystems.sprites_fs import (
        SpritesFS,
        SpritesInfo,
        SpritesPath,
    )
    from upathtools.filesystems.sandbox_filesystems.srt_fs import SRTFS, SRTInfo, SRTPath
    from upathtools.filesystems.sandbox_filesystems.vercel_fs import (
        VercelFS,
        VercelInfo,
        VercelPath,
    )

    SandboxFilesystem = (
        BeamFS
        | CloudflareFS
        | CodexFS
        | DaytonaFS
        | E2BFS
        | HopXFS
        | MicrosandboxFS
        | ModalFS
        | SpritesFS
        | SRTFS
        | VercelFS
    )

__getattr__, __dir__ = lazy_exports(
    __name__,
    {
        "BeamFS": "upathtools.filesystems.sandbox_filesystems.beam_fs",
        "BeamInfo": "upathtools.filesystems.sandbox_filesystems.beam_fs",
        "BeamPath": "upathtools.filesystems.sandbox_filesystems.beam_fs",
        "CodexFS": "upathtools.filesystems.sandbox_filesystems.codex_fs",
        "CodexInfo": "upathtools.filesystems.sandbox_filesystems.codex_fs",
        "CodexPath": "upathtools.filesystems.sandbox_filesystems.codex_fs",
        "CloudflareFS": "upathtools.filesystems.sandbox_filesystems.cloudflare_fs",
        "CloudflareInfo": "upathtools.filesystems.sandbox_filesystems.cloudflare_fs",
        "CloudflarePath": "upathtools.filesystems.sandbox_filesystems.cloudflare_fs",
        "DaytonaFS": "upathtools.filesystems.sandbox_filesystems.daytona_fs",
        "DaytonaInfo": "upathtools.filesystems.sandbox_filesystems.daytona_fs",
        "DaytonaPath": "upathtools.filesystems.sandbox_filesystems.daytona_fs",
        "E2BFS": "upathtools.filesystems.sandbox_filesystems.e2b_fs",
        "E2BInfo": "upathtools.filesystems.sandbox_filesystems.e2b_fs",
        "E2BPath": "upathtools.filesystems.sandbox_filesystems.e2b_fs",
        "HopXFS": "upathtools.filesystems.sandbox_filesystems.hopx_fs",
        "HopXInfo": "upathtools.filesystems.sandbox_filesystems.hopx_fs",
        "HopXPath": "upathtools.filesystems.sandbox_filesystems.hopx_fs",
        "MicrosandboxFS": "upathtools.filesystems.sandbox_filesystems.microsandbox_fs",
        "MicrosandboxInfo": "upathtools.filesystems.sandbox_filesystems.microsandbox_fs",
        "MicrosandboxPath": "upathtools.filesystems.sandbox_filesystems.microsandbox_fs",
        "ModalFS": "upathtools.filesystems.sandbox_filesystems.modal_fs",
        "ModalInfo": "upathtools.filesystems.sandbox_filesystems.modal_fs",
        "ModalPath": "upathtools.filesystems.sandbox_filesystems.modal_fs",
        "SpritesFS": "upathtools.filesystems.sandbox_filesystems.sprites_fs",
        "SpritesInfo": "upathtools.filesystems.sandbox_filesystems.sprites_fs",
        "SpritesPath": "upathtools.filesystems.sandbox_filesystems.sprites_fs",
        "SRTFS": "upathtools.filesystems.sandbox_filesystems.srt_fs",
        "SRTInfo": "upathtools.filesystems.sandbox_filesystems.srt_fs",
        "SRTPath": "upathtools.filesystems.sandbox_filesystems.srt_fs",
        "VercelFS": "upathtools.filesystems.sandbox_filesystems.vercel_fs",
        "VercelInfo": "upathtools.filesystems.sandbox_filesystems.vercel_fs",
        "VercelPath": "upathtools.filesystems.sandbox_filesystems.vercel_fs",
    },
    {
        "SandboxFilesystem": (
            "BeamFS",
            "CloudflareFS",
            "CodexFS",
            "DaytonaFS",
            "E2BFS",
            "HopXFS",
            "MicrosandboxFS",
            "ModalFS",
            "SpritesFS",
            "SRTFS",
            "VercelFS",
        ),
    },
)

__all__ = [
    "E2BFS",
    "SRTFS",
    "BeamFS",
    "BeamInfo",
    "BeamPath",
    "CloudflareFS",
    "CloudflareInfo",
    "CloudflarePath",
    "CodexFS",
    "CodexInfo",
    "CodexPath",
    "DaytonaFS",
    "DaytonaInfo",
    "DaytonaPath",
    "E2BInfo",
    "E2BPath",
    "HopXFS",
    "HopXInfo",
    "HopXPath",
    "MicrosandboxFS",
    "MicrosandboxInfo",
    "MicrosandboxPath",
    "ModalFS",
    "ModalInfo",
    "ModalPath",
    "SRTInfo",
    "SRTPath",
    "SandboxFilesystem",
    "SpritesFS",
    "SpritesInfo",
    "SpritesPath",
    "VercelFS",
    "VercelInfo",
    "VercelPath",
]
