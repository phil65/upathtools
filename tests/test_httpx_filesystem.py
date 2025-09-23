"""Test module for fsspec-httpx filesystem implementation."""

from __future__ import annotations

from collections import ChainMap
import contextlib
import gzip
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import pickle
import threading
import time
from types import SimpleNamespace
from typing import Any, ClassVar

import fsspec.asyn
import fsspec.registry
import fsspec.utils
import pytest

from upathtools import HTTPFileSystem
from upathtools.filesystems.httpx_fs import HTTPStreamFile as OurHTTPStreamFile


logger = logging.getLogger(__name__)
fsspec.register_implementation("http", HTTPFileSystem, clobber=True)

data = b"\n".join([b"some test data"] * 1000)
listing = open(  # noqa: PTH123, SIM115
    os.path.join(os.path.dirname(__file__), "data", "listing.html"),  # noqa: PTH118, PTH120
    "rb",
).read()
win = os.name == "nt"


def _make_realfile(baseurl):
    return f"{baseurl}/index/realfile"


def _make_index_listing(baseurl):
    realfile = _make_realfile(baseurl)
    return b'<a href="%s">Link</a>' % realfile.encode()


def _make_listing(*paths):
    def _make_listing_port(baseurl):
        return "\n".join(
            f'<a href="{baseurl}{f}">Link_{i}</a>' for i, f in enumerate(paths)
        ).encode()

    return _make_listing_port


@pytest.fixture
def reset_files():
    yield

    # Reset the newly added files after the
    # test is completed.
    HTTPTestHandler.dynamic_files.clear()


class HTTPTestHandler(BaseHTTPRequestHandler):
    static_files: ClassVar = {
        "/index/realfile": data,
        "/index/otherfile": data,
        "/index": _make_index_listing,
        "/data/20020401": listing,
        "/simple/": _make_listing("/simple/file", "/simple/dir/"),
        "/simple/file": data,
        "/simple/dir/": _make_listing("/simple/dir/file"),
        "/simple/dir/file": data,
    }
    dynamic_files: ClassVar[dict[str, Any]] = {}

    files = ChainMap(dynamic_files, static_files)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _make_index_response(self, baseurl: str, file_path: str) -> bytes:
        """Generate proper HTML index listing."""
        html = ["<!DOCTYPE html><html><body>"]

        if file_path == "/index/":
            # Special case for /index/ to return realfile
            realfile = _make_realfile(baseurl)
            html.append(f'<a href="{realfile}">realfile</a>')
        elif file_path == "/simple/":
            html.append(f'<a href="{baseurl}/simple/file">file</a>')
            html.append(f'<a href="{baseurl}/simple/dir/">dir/</a>')
        elif file_path == "/simple/dir/":
            html.append(f'<a href="{baseurl}/simple/dir/file">file</a>')
        elif file_path == "/data/20020401/":
            # Use the provided listing data
            return listing

        html.append("</body></html>")
        return "\n".join(html).encode()

    def _respond(self, code=200, headers=None, data=b""):
        headers = headers or {}
        headers.update({"User-Agent": "test"})
        self.send_response(code)
        for k, v in headers.items():
            # Ensure header values are stripped of whitespace
            self.send_header(k, str(v).strip())
        self.end_headers()
        if data:
            self.wfile.write(data)

    def do_GET(self):  # noqa: PLR0911
        assert isinstance(self.server, HTTPServer)
        baseurl = f"http://{self.server.server_name}:{self.server.server_port}"
        file_path = self.path
        logger.debug("Test server received request for: %s", file_path)

        # First check for exact match
        file_data: Any = self.files.get(file_path)

        # If not found and path ends with /, try without the slash
        if file_data is None and file_path.endswith("/"):
            file_data = self.files.get(file_path.rstrip("/"))

        # If not found and path doesn't end with /, try with the slash
        if file_data is None and not file_path.endswith("/"):
            file_data = self.files.get(file_path + "/")

        if callable(file_data):
            file_data = file_data(baseurl)

        if file_data is None:
            return self._respond(404)
        if "give_path" in self.headers:
            return self._respond(200, data=json.dumps({"path": self.path}).encode())
        if "redirect" in self.headers and file_path != "/index/realfile":
            new_url = _make_realfile(baseurl)
            return self._respond(301, {"Location": new_url})
        if file_data is None:
            return self._respond(404)

        status = 200

        content_range = f"bytes 0-{len(file_data) - 1}/{len(file_data)}"
        if ("Range" in self.headers) and ("ignore_range" not in self.headers):
            ran = self.headers["Range"]
            _b, ran = ran.split("=")
            start, end = ran.split("-")
            if start:
                content_range = f"bytes {start}-{end}/{len(file_data)}"
                file_data = file_data[int(start) : (int(end) + 1) if end else None]
            else:
                # suffix only
                length = len(file_data)
                content_range = f"bytes {length - int(end)}-{length - 1}/{length}"
                file_data = file_data[-int(end) :]
            if "use_206" in self.headers:
                status = 206
        if "give_length" in self.headers:
            if "gzip_encoding" in self.headers:
                file_data = gzip.compress(file_data)
                response_headers = {
                    "Content-Length": len(file_data),
                    "Content-Encoding": "gzip",
                }
            else:
                response_headers = {"Content-Length": len(file_data)}
            self._respond(status, response_headers, file_data)
            return None
        if "give_range" in self.headers:
            self._respond(status, {"Content-Range": content_range}, file_data)
            return None
        if "give_mimetype" in self.headers:
            self._respond(status, {"Content-Type": "text/html; charset=utf-8"}, file_data)
            return None
        self._respond(status, data=file_data)
        return None

    def do_POST(self):
        length = self.headers.get("Content-Length")
        file_path = self.path.rstrip("/")
        if length is None:
            assert self.headers.get("Transfer-Encoding") == "chunked"
            self.files[file_path] = b"".join(self.read_chunks())
        else:
            self.files[file_path] = self.rfile.read(int(length))
        self._respond(200)

    do_PUT = do_POST  # noqa: N815

    def read_chunks(self):
        length = -1
        while length != 0:
            line = self.rfile.readline().strip()
            length = 0 if len(line) == 0 else int(line, 16)
            yield self.rfile.read(length)
            self.rfile.readline()

    def do_HEAD(self):
        r_headers: dict[str, Any] = {}
        if "head_not_auth" in self.headers:
            r_headers["Content-Length"] = 123
            return self._respond(403, r_headers, b"not authorized for HEAD request")
        if "head_ok" not in self.headers:
            return self._respond(405)

        file_path = self.path.rstrip("/")
        file_data = self.files.get(file_path)
        if file_data is None:
            return self._respond(404)

        if ("give_length" in self.headers) or ("head_give_length" in self.headers):
            if "zero_length" in self.headers:
                r_headers["Content-Length"] = 0
            elif "gzip_encoding" in self.headers:
                file_data = gzip.compress(file_data)
                r_headers["Content-Encoding"] = "gzip"
                r_headers["Content-Length"] = len(file_data)
            else:
                r_headers["Content-Length"] = len(file_data)
        elif "give_range" in self.headers:
            r_headers["Content-Range"] = f"0-{len(file_data) - 1}/{len(file_data)}"
        elif "give_etag" in self.headers:
            r_headers["ETag"] = "xxx"

        if self.headers.get("accept_range") == "none":
            r_headers["Accept-Ranges"] = "none"

        self._respond(200, r_headers)
        return None


@contextlib.contextmanager
def serve():
    server_address = ("127.0.0.1", 0)
    httpd = HTTPServer(server_address, HTTPTestHandler)
    th = threading.Thread(target=httpd.serve_forever)
    th.daemon = True
    th.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


@pytest.fixture(scope="module")
def server():
    with serve() as s:
        server = SimpleNamespace(address=s, realfile=_make_realfile(s))
        yield server


def test_list(server):
    h = fsspec.filesystem("http")
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_invalid_args(server):
    with pytest.raises(TypeError):  # noqa: PT012
        h = fsspec.filesystem("http", use_foobar=True)
        h.glob(server.address + "/index/*")


def test_list_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_expiry_time_cached(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=30)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]

    # Verify cache content.
    assert len(h.dircache) == 1

    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_expiry_time_purged(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=0.3)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]
    assert len(h.dircache) == 1

    # Verify cache content.
    assert server.address + "/index/" in h.dircache
    assert len(h.dircache.get(server.address + "/index/")) == 1

    # Wait beyond the TTL / cache expiry time.
    time.sleep(0.31)

    # Verify that the cache item should have been purged.
    cached_items = h.dircache.get(server.address + "/index/")
    assert cached_items is None

    # Verify that after clearing the item from the cache,
    # it can get populated again.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]
    cached_items = h.dircache.get(server.address + "/index/")
    assert len(cached_items) == 1


def test_list_cache_reuse(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]

    # Verify cache content.
    assert len(h.dircache) == 1

    # Verify another instance without caching enabled does not have cache content.
    h = fsspec.filesystem("http", use_listings_cache=False)
    assert not h.dircache

    # Verify that yet another new instance, with caching enabled,
    # will see the same cache content again.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)
    assert len(h.dircache) == 1

    # However, yet another instance with a different expiry time will also not have
    # any valid cache content.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=666)
    assert len(h.dircache) == 0


def test_ls_raises_filenotfound(server):
    h = fsspec.filesystem("http")
    with pytest.raises(FileNotFoundError):
        h.ls(server.address + "/not-a-key")


def test_list_cache_with_max_paths(server):
    h = fsspec.filesystem("http", use_listings_cache=True, max_paths=5)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_skip_instance_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True, skip_instance_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_glob_return_subfolders(server):
    h = fsspec.filesystem("http")
    out = h.glob(server.address + "/simple/*")
    assert set(out) == {
        server.address + "/simple/dir/",
        server.address + "/simple/file",
    }


def test_isdir(server):
    h = fsspec.filesystem("http")
    assert h.isdir(server.address + "/index/")
    assert not h.isdir(server.realfile)
    assert not h.isdir(server.address + "doesnotevenexist")


def test_policy_arg(server):
    h = fsspec.filesystem("http", size_policy="get")
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_exists(server):
    h = fsspec.filesystem("http")
    assert not h.exists(server.address + "/notafile")
    with pytest.raises(FileNotFoundError):
        h.cat(server.address + "/notafile")


def test_read(server):
    h = fsspec.filesystem("http")
    out = server.realfile
    with h.open(out, "rb") as f:
        assert f.read() == data
    with h.open(out, "rb", block_size=0) as f:
        assert f.read() == data
    with h.open(out, "rb") as f:
        assert f.read(100) + f.read() == data


def test_file_pickle(server):
    # via HTTPFile
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    out = server.realfile

    with fsspec.open(out, headers={"give_length": "true", "head_ok": "true"}) as f:
        pic = pickle.loads(pickle.dumps(f))
        assert pic.read() == data

    with h.open(out, "rb") as f:
        pic = pickle.dumps(f)
        assert f.read() == data
    with pickle.loads(pic) as f:
        assert f.read() == data

    # via HTTPStreamFile
    h = fsspec.filesystem("http")
    out = server.realfile
    with h.open(out, "rb") as f:
        out = pickle.dumps(f)
        assert f.read() == data
    with pickle.loads(out) as f:
        assert f.read() == data


def test_methods(server):
    h = fsspec.filesystem("http")
    url = server.realfile
    assert h.exists(url)
    assert h.cat(url) == data


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"give_length": "true"},
        {"give_length": "true", "head_ok": "true"},
        {"give_range": "true"},
        {"give_length": "true", "head_not_auth": "true"},
        {"give_range": "true", "head_not_auth": "true"},
        {"use_206": "true", "head_ok": "true", "head_give_length": "true"},
        {"use_206": "true", "give_length": "true"},
        {"use_206": "true", "give_range": "true"},
    ],
)
def test_random_access(server, headers):
    h = fsspec.filesystem("http", headers=headers)
    url = server.realfile
    with h.open(url, "rb") as f:
        if headers:
            assert f.size == len(data)
        assert f.read(5) == data[:5]

        if headers:
            f.seek(5, 1)
            assert f.read(5) == data[10:15]
        else:
            with pytest.raises(ValueError):  # noqa: PT011
                f.seek(5, 1)
    assert f.closed


@pytest.mark.parametrize(
    "headers",
    [
        # HTTPFile seeks, response headers lack size, assumed no range support
        {"head_ok": "true", "head_give_length": "true"},
        # HTTPFile seeks, response is not a range
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
        # HTTPStreamFile does not seek (past 0)
        {"accept_range": "none", "head_ok": "true", "give_length": "true"},
    ],
)
def test_no_range_support(server, headers):
    h = fsspec.filesystem("http", headers=headers)
    url = server.realfile
    with h.open(url, "rb") as f:
        # Random access is not possible if the server doesn't respect Range
        with pytest.raises(ValueError):  # noqa: PT011, PT012
            f.seek(5)
            f.read(10)

        # Reading from the beginning should still work
        f.seek(0)
        assert f.read(10) == data[:10]


def test_stream_seek(server):
    h = fsspec.filesystem("http")
    url = server.realfile
    with h.open(url, "rb") as f:
        f.seek(0)  # is OK
        data1 = f.read(5)
        assert len(data1) == 5  # noqa: PLR2004
        f.seek(5)
        f.seek(0, 1)
        data2 = f.read()
        assert data1 + data2 == data


def test_mapper_url(server):
    h = fsspec.filesystem("http")
    mapper = h.get_mapper(server.address + "/index/")
    assert mapper.root.startswith("http:")
    assert list(mapper)

    mapper2 = fsspec.get_mapper(server.address + "/index/")
    assert mapper2.root.startswith("http:")
    assert list(mapper) == list(mapper2)


def test_content_length_zero(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "zero_length": "true"})
    url = server.realfile

    with h.open(url, "rb") as f:
        assert f.read() == data


def test_content_encoding_gzip(server):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "gzip_encoding": "true"}
    )
    url = server.realfile
    with h.open(url, "rb") as f:
        assert isinstance(f, OurHTTPStreamFile)


def test_download(server, tmpdir):
    # Remove space after "true"
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    url = server.realfile
    fn = str(tmpdir / "afile")
    h.get(url, fn)
    assert open(fn, "rb").read() == data  # noqa: PTH123, SIM115


def test_multi_download(server, tmpdir):
    # Remove space after "true"
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    fna = tmpdir / "afile"
    fnb = tmpdir / "bfile"
    h.get([urla, urlb], [fna, fnb])
    assert open(fna, "rb").read() == data  # noqa: PTH123, SIM115
    assert open(fnb, "rb").read() == data  # noqa: PTH123, SIM115


def test_ls(server):
    h = fsspec.filesystem("http")
    ls = h.ls(server.address + "/data/20020401/", detail=False)
    nc = server.address + "/data/20020401/GRACEDADM_CLSM0125US_7D.A20020401.030.nc4"
    assert nc in ls
    assert len(ls) == 11  # noqa: PLR2004
    assert all(u["type"] == "file" for u in h.ls(server.address + "/data/20020401/"))
    assert h.glob(server.address + "/data/20020401/*.nc4") == [nc]


def test_mcat(server):
    # Remove space after "true"
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_cat_file_range(server):
    # Remove space after "true"
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    assert h.cat(urla, start=1, end=10) == data[1:10]
    assert h.cat(urla, start=1) == data[1:]

    assert h.cat(urla, start=-10) == data[-10:]
    assert h.cat(urla, start=-10, end=-2) == data[-10:-2]

    assert h.cat(urla, end=-10) == data[:-10]


def test_cat_file_range_numpy(server):
    np = pytest.importorskip("numpy")
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server.realfile
    assert h.cat(urla, start=np.int8(1), end=np.int8(10)) == data[1:10]
    out = h.cat_ranges([urla, urla], starts=np.array([1, 5]), ends=np.array([10, 15]))
    assert out == [data[1:10], data[5:15]]


def test_mcat_cache(server):
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    fs = fsspec.filesystem("simplecache", target_protocol="http")
    assert fs.cat([urla, urlb]) == {urla: data, urlb: data}


def test_mcat_expand(server):
    # Remove space after "true"
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    out = h.cat(server.address + "/index/*")
    assert out == {server.realfile: data}


def test_info(server):
    fs = fsspec.filesystem("http", headers={"give_etag": "true", "head_ok": "true"})
    info = fs.info(server.realfile)
    assert info["ETag"] == "xxx"

    fs = fsspec.filesystem("http", headers={"give_mimetype": "true"})
    info = fs.info(server.realfile)
    assert info["mimetype"] == "text/html"


if __name__ == "__main__":
    pytest.main([__file__, "--log-level", "DEBUG"])
