from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fsspec.core import _un_chain, stringify_path
from fsspec.registry import get_filesystem_class

from upathtools.filesystems.async_local_fs import AsyncLocalFileSystem


if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem


def filesystem(protocol: str, **storage_options: Any) -> AbstractFileSystem:
    """Instantiate filesystems for given protocol and arguments.

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    cls = get_filesystem_class(protocol)
    return cls(**storage_options)


def url_to_fs(url: str, **kwargs: Any) -> tuple[AbstractFileSystem, str]:
    """Turn fully-qualified and potentially chained URL into filesystem instance.

    Args:
        url : str
            The fsspec-compatible URL
        **kwargs: dict
            Extra options that make sense to a particular storage connection, e.g.
            host, port, username, password, etc.

    Returns:
        filesystem : FileSystem
            The new filesystem discovered from ``url`` and created with
            ``**kwargs``.
        urlpath : str
            The file-systems-specific URL for ``url``.
    """
    url = stringify_path(url)
    # non-FS arguments that appear in fsspec.open()
    # inspect could keep this in sync with open()'s signature
    known_kwargs = {
        "compression",
        "encoding",
        "errors",
        "expand",
        "mode",
        "name_function",
        "newline",
        "num",
    }
    kwargs = {k: v for k, v in kwargs.items() if k not in known_kwargs}
    chain = _un_chain(url, kwargs)
    inkwargs: dict[str, Any] = {}
    # Reverse iterate the chain, creating a nested target_* structure
    for i, ch in enumerate(reversed(chain)):
        urls, protocol, kw = ch
        if i == len(chain) - 1:
            inkwargs = dict(**kw, **inkwargs)
            continue
        inkwargs["target_options"] = dict(**kw, **inkwargs)
        inkwargs["target_protocol"] = protocol
        inkwargs["fo"] = urls
    urlpath, protocol, _ = chain[0]
    if protocol in {"", "file"}:
        fs = AsyncLocalFileSystem(asynchronous=True)
    else:
        fs = filesystem(protocol, **inkwargs)
    return fs, urlpath


if __name__ == "__main__":
    fs, urlpath = url_to_fs("file:///path/to/file.txt")
    print(fs, urlpath)
