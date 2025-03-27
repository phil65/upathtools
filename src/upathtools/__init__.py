__version__ = "0.6.0"

from upathtools.async_ops import read_path, read_folder, list_files, read_folder_as_text
from upathtools.httpx_fs import HttpPath, HTTPFileSystem


def register_http_filesystems():
    """Register HTTP filesystems."""
    from fsspec import register_implementation
    from upath import registry

    register_implementation("http", HTTPFileSystem, clobber=True)
    registry.register_implementation("http", HttpPath, clobber=True)
    register_implementation("https", HTTPFileSystem, clobber=True)
    registry.register_implementation("https", HttpPath, clobber=True)


def register_all_filesystems():
    """Register all filesystem implementations provided by upathtools."""
    from fsspec import register_implementation
    from upath import registry

    # Import all filesystem implementations to ensure they're available
    from upathtools.cli_fs import CliFS, CliPath
    from upathtools.distribution_fs import DistributionFS, DistributionPath
    from upathtools.flat_union_fs import FlatUnionFileSystem, FlatUnionPath
    from upathtools.markdown_fs import MarkdownFS, MarkdownPath
    from upathtools.module_fs import ModuleFS, ModulePath
    from upathtools.package_fs import PackageFS, PackagePath
    from upathtools.python_ast_fs import AstPath, PythonAstFS
    from upathtools.union_fs import UnionFileSystem, UnionPath

    # Register HTTP filesystems
    register_http_filesystems()
    # Register other filesystems
    register_implementation("cli", CliFS, clobber=True)
    registry.register_implementation("cli", CliPath, clobber=True)

    register_implementation("distribution", DistributionFS, clobber=True)
    registry.register_implementation("distribution", DistributionPath, clobber=True)

    register_implementation("flatunion", FlatUnionFileSystem, clobber=True)
    registry.register_implementation("flatunion", FlatUnionPath, clobber=True)

    register_implementation("md", MarkdownFS, clobber=True)
    registry.register_implementation("md", MarkdownPath, clobber=True)

    register_implementation("mod", ModuleFS, clobber=True)
    registry.register_implementation("mod", ModulePath, clobber=True)

    register_implementation("pkg", PackageFS, clobber=True)
    registry.register_implementation("pkg", PackagePath, clobber=True)

    register_implementation("ast", PythonAstFS, clobber=True)
    registry.register_implementation("ast", AstPath, clobber=True)

    register_implementation("union", UnionFileSystem, clobber=True)
    registry.register_implementation("union", UnionPath, clobber=True)


__all__ = [
    "HTTPFileSystem",
    "HttpPath",
    "list_files",
    "read_folder",
    "read_folder_as_text",
    "read_path",
    "register_all_filesystems",
    "register_http_filesystems",
]
