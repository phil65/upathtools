# AGENTS.md

## Overview

`upathtools` extends [fsspec](https://filesystem-spec.readthedocs.io/) and
[universal-pathlib](https://github.com/fsspec/universal_pathlib) (`UPath`) with async helpers,
typed filesystem base classes and ~40 filesystem implementations (virtual/file-based, remote
APIs, sandboxes, combining filesystems). A second package, `upathtools_config`, holds
lightweight pydantic config models that create those filesystems. Many other projects
(agentpool, docler, slashed, ...) depend on it: treat public names, entry points and
signatures as API.

## Architecture

- `src/upathtools/__init__.py`: public API (`AsyncUPath`, `read_path`, `read_folder`,
  `list_files`, `read_folder_as_text`, `fsspec_grep`, `to_upath`, `upath_to_fs`, `url_to_fs`,
  `register_all_filesystems`, ...).
- `async_upath.py`: `AsyncUPath` (ProxyUPath with `a*` methods; falls back to threads for sync fs).
- `async_ops.py`: batch/async functions, `get_async_fs` / `to_async_fs` (wraps sync fs).
- `helpers.py`: sync helpers (`to_upath`, `copy`, `write_file`, `multi_glob`, `clean_directory`).
- `core.py`: `filesystem()` / `url_to_fs()` wrappers around fsspec.
- `cli_ops.py` (grep/find/ls/... as async functions + sync wrappers) and `cli_parser.py`
  (`execute_cli`, parses `"grep TODO . -r"` strings; used by `BaseUPath.cli()` / `fs.cli()`).
- `filetree/`: directory tree rendering (`get_directory_tree`) + jinjarope filter entry point.
- `pydantic_type.py`: pydantic `UPath`, `UPathFile`, `UPathDir`, `UPathExists` types.
- `monty_os.py`: adapter from fsspec filesystems to pydantic-monty's `AbstractOS` (extra `monty`).
- `filesystems/base/`: core abstractions
  - `basefilesystem.py`: `BaseAsyncFileSystem[TPath, TInfoDict]`, `BaseFileSystem` (sync);
    provide `get_upath()`, `get_tree()`, `cli()`, `_grep`, `jq`, `diff`, `register_fs()`.
  - `baseupath.py`: `BaseUPath[TInfoDict]` (UPath with async methods, `cli()`, tree).
  - `filefilesystem.py`: `BaseAsyncFileFileSystem` + `ProbeResult` for filesystems that expose
    the inside of one file (`supported_extensions`, `probe_content`, `from_content`).
  - `wrapper.py`: `WrapperFileSystem` (wraps another fs, info callbacks, `mount()`).
- `filesystems/` implementations, all re-exported from `filesystems/__init__.py`:
  - `file_filesystems/`: markdown, sqlite, tree-sitter, openapi, jsonschema, tar.
  - `remote_filesystems/`: gist, wiki, issues, gitlab, linear, notion, appwrite, mcp, mcptools.
  - `sandbox_filesystems/`: beam, codex, cloudflare, daytona, e2b, hopx, microsandbox, modal,
    sprites, srt, vercel, pyodide (`_shell_search.py` = shared shell-based grep/find).
  - `combining_filesystems/`: union (mount points), flatunion (merged view), overlay (layers).
  - top level: `package_fs.py` (`pkg://`), `distribution_fs.py`, `basemodel_fs.py`,
    `basemodel_instance_fs.py`, `httpx_fs.py`, `async_local_fs.py`, `isolated_memory_fs.py`,
    `delegating_fs.py` (`file.md::Section` access via file filesystems), `auto_expand_fs.py`,
    `skills_fs.py`.
- `src/upathtools_config/`: `base.py` (`FileSystemConfig.create_fs()`), one module per category;
  `FilesystemConfigType` is the discriminated union (`type` field).

Pattern per filesystem: `XInfo(TypedDict)` + `XPath(BaseUPath[XInfo])` +
`XFileSystem(BaseAsyncFileSystem[XPath, XInfo])` with `protocol` and `upath_cls`, registered in
`pyproject.toml` under both `[project.entry-points."fsspec.specs"]` and
`[project.entry-points."universal_pathlib.implementations"]`.

## Where to look

| Task | Location |
|------|----------|
| Add a filesystem | `src/upathtools/filesystems/<category>/`, export in `filesystems/__init__.py`, entry points + extra in `pyproject.toml`, config in `src/upathtools_config/` |
| Async path behaviour | `src/upathtools/async_upath.py`, `filesystems/base/baseupath.py` |
| `get_upath` / tree / grep on filesystems | `filesystems/base/basefilesystem.py` |
| `path.cli("...")` commands | `cli_parser.py`, `cli_ops.py` |
| Config models | `src/upathtools_config/*_configs.py` |
| Docs pages | `docs/*.md` (zensical, `mkdocs.yml`; `/// mknodes` blocks via `mknodes.mdext`) |
| Usage examples for sandboxes | `examples/` |

## Commands

```bash
uv sync --all-extras                 # setup (CI: --no-group docs --no-group lint)
uv run pytest                        # tests (integration tests deselected by default)
uv run pytest -m integration         # needs API keys / services
uvx ruff check . && uvx ruff format --check .
uv run ty check                      # must be clean, warnings fail
uv run deptry src                    # dependency check (config in pyproject)
uv run zensical build                # docs (duty: build / serve)
uv run duty lint                     # ruff fix + format + ty
```

## Conventions

- Python >= 3.13, PEP 695 generics, `from __future__ import annotations`, Google docstrings,
  line length 100, ruff preview rules (see `pyproject.toml`).
- Optional backends are imported lazily inside methods or under `TYPE_CHECKING`; `import
  upathtools` and `import upathtools.filesystems` must work with only the core dependencies.
- Async-first: implement `async def _ls/_info/_cat_file/...`; sync methods come from fsspec's
  `sync_wrapper` machinery. Never call an async filesystem's sync API from inside its loop.
- Tests: `tests/`, pytest-asyncio in auto mode (session loop scope), sandbox tests in
  `tests/sandbox_filesystems/`; mark tests that need network/credentials with `@pytest.mark.integration`.
- ty rules relaxed for fsspec override inconsistencies (`[tool.ty.rules]`); use targeted
  `# ty: ignore[rule]` otherwise.

## Specs

Interfaces implemented and remote APIs wrapped. Sandbox filesystems use the same vendor SDKs as exxec (see exxec's AGENTS.md Specs); generic fsspec backends (s3, gcs, azure, ...) are not listed.

| Spec | URL | Targets | Code | Update check |
|---|---|---|---|---|
| fsspec `AbstractFileSystem` / `AsyncFileSystem` | https://filesystem-spec.readthedocs.io/en/latest/api.html | unpinned (tracks latest fsspec / universal-pathlib) | `src/upathtools/filesystems/base/`, all filesystems | https://filesystem-spec.readthedocs.io/en/latest/changelog.html |
| MCP resources / tools (via fastmcp) | https://modelcontextprotocol.io/specification/latest | unpinned (follows the installed `mcp` SDK) | `src/upathtools/filesystems/remote_filesystems/mcp_fs.py`, `mcp_tools_fs.py` | same URL |
| Agent Skills (`SKILL.md` frontmatter) | https://agentskills.io/specification | unpinned (tracks latest) | `src/upathtools/filesystems/skills_fs.py` | same URL |
| GitHub REST (contents, gists, issues; wiki via git) | https://docs.github.com/en/rest/repos/contents | unpinned (default REST API version) | `src/upathtools/filesystems/remote_filesystems/github_fs.py`, `gist_fs.py`, `issue_fs.py`, `wiki_fs.py` | https://docs.github.com/en/rest/about-the-rest-api/api-versions |
| GitLab repository files (via `python-gitlab`) | https://docs.gitlab.com/api/repository_files/ | API v4 (python-gitlab) | `src/upathtools/filesystems/remote_filesystems/gitlab_fs.py` | same URL |
| Linear GraphQL API | https://linear.app/developers/graphql | unpinned (`https://api.linear.app/graphql`) | `src/upathtools/filesystems/remote_filesystems/linear_fs.py` | same URL |
| Notion API (via `notion-client`) | https://developers.notion.com/reference/intro | unpinned (SDK default `Notion-Version`) | `src/upathtools/filesystems/remote_filesystems/notion_fs.py` | https://developers.notion.com/page/changelog |
| Appwrite Storage (via `appwrite`) | https://appwrite.io/docs/references/cloud/server-python/storage | unpinned (tracks latest SDK) | `src/upathtools/filesystems/remote_filesystems/appwrite_fs.py` | same URL |
| Codex app-server `fs/*` (via codexed) | https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md | whatever codexed targets (see codexed's AGENTS.md Specs) | `src/upathtools/filesystems/sandbox_filesystems/codex_fs.py` | codexed releases |
| OpenAPI / JSON Schema documents (read-only browsing) | https://spec.openapis.org/oas/latest.html | unpinned (OpenAPI 3.x, any JSON Schema draft) | `src/upathtools/filesystems/file_filesystems/openapi_fs.py`, `jsonschema_fs.py` | https://json-schema.org/specification |

## Pitfalls

- Copier-managed (template `copier-phil65`): `.github/workflows/`, `duties.py`, `mkdocs.yml`,
  `.pre-commit-config.yaml`, parts of `pyproject.toml`. Add dependencies only below the
  `# Only add below (Copier)` markers; update via `copier update`, not by hand.
- `pyproject.toml` is formatted with tombi (`uvx tombi format pyproject.toml`).
- upath only accepts a URL in a path subclass if the protocol is registered for it; entry point
  names don't always equal the fs protocol (`markdown` vs `md`, `issues` vs `githubissues`).
  `get_upath()` registers missing ones; constructing `XPath("proto://...")` directly may not.
- Extras with platform markers: beam (no Windows on 3.14), daytona (< 3.14, pinned < 2),
  microsandbox (no Windows); ty allows these imports to be unresolved.
- Env vars used by remote filesystems/tests: `GITHUB_TOKEN`/`GH_TOKEN`, `GITLAB_TOKEN`,
  `LINEAR_API_KEY`, `NOTION_API_KEY`, `NOTION_PARENT_PAGE_ID`, `APPWRITE_*`.
- Pre-commit runs ruff, tombi, ty and pytest on commit.
