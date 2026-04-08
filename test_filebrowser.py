"""Comprehensive test suite for filebrowser.py — all functionality."""

import io
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.client import HTTPConnection
from unittest import mock

import pytest

# --- Bootstrap: stub `cgi` if missing (removed in Python 3.13+) ---
if "cgi" not in sys.modules:
    try:
        import cgi  # noqa: F401
    except ModuleNotFoundError:
        cgi_stub = type(sys)("cgi")
        cgi_stub.FieldStorage = None  # placeholder; POST upload tests mock it
        sys.modules["cgi"] = cgi_stub

# Patch ROOT_DIR before importing filebrowser so safe_path uses our tmp dir
_TMPDIR = tempfile.mkdtemp(prefix="fb_test_")

import filebrowser as fb  # noqa: E402

# Override module-level ROOT_DIR
fb.ROOT_DIR = _TMPDIR


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(autouse=True)
def _reset_root(tmp_path):
    """Give each test a fresh ROOT_DIR and reset caches."""
    fb.ROOT_DIR = str(tmp_path)
    fb._bucket_name_cache.clear()
    yield


@pytest.fixture
def populated_dir(tmp_path):
    """Create a directory tree with known files and folders."""
    root = tmp_path
    # dirs
    (root / "alpha").mkdir()
    (root / "beta").mkdir()
    # files
    (root / "file1.txt").write_text("hello")
    (root / "file2.log").write_text("world!")
    (root / "alpha" / "nested.txt").write_text("inside alpha")
    return root


@pytest.fixture
def large_dir(tmp_path):
    """Create a directory with more than PAGE_SIZE entries."""
    root = tmp_path
    for i in range(fb.PAGE_SIZE + 50):
        (root / f"item_{i:05d}.dat").write_text(f"data {i}")
    return root


@pytest.fixture
def server(tmp_path):
    """Start a threaded HTTP server on a random port, yield (host, port), stop after test."""
    fb.ROOT_DIR = str(tmp_path)
    srv = fb.ThreadedHTTPServer(("127.0.0.1", 0), fb.Handler)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield "127.0.0.1", port
    srv.shutdown()


def http_get(host, port, path):
    """Helper: GET and return (status, headers_dict, body_bytes)."""
    conn = HTTPConnection(host, port, timeout=10)
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read()
    headers = dict(resp.getheaders())
    conn.close()
    return resp.status, headers, body


def http_post_form(host, port, path, data: dict):
    """Helper: POST url-encoded form and return (status, headers_dict, body)."""
    encoded = urllib.parse.urlencode(data).encode()
    conn = HTTPConnection(host, port, timeout=10)
    conn.request("POST", path, body=encoded,
                 headers={"Content-Type": "application/x-www-form-urlencoded",
                           "Content-Length": str(len(encoded))})
    resp = conn.getresponse()
    body = resp.read()
    headers = dict(resp.getheaders())
    conn.close()
    return resp.status, headers, body


# ============================================================
# 1. safe_path — path traversal prevention
# ============================================================

class TestSafePath:
    def test_normal_path(self, tmp_path):
        assert fb.safe_path("/bucket/file.txt") == os.path.join(str(tmp_path), "bucket", "file.txt")

    def test_root(self, tmp_path):
        assert fb.safe_path("/") == str(tmp_path)

    def test_empty_string(self, tmp_path):
        assert fb.safe_path("") == str(tmp_path)

    def test_traversal_dot_dot(self, tmp_path):
        result = fb.safe_path("/../../../etc/passwd")
        assert result == str(tmp_path)

    def test_traversal_encoded(self, tmp_path):
        result = fb.safe_path("/bucket/../../etc/shadow")
        assert result == str(tmp_path)  # blocked — collapses to ROOT_DIR

    def test_deep_nested(self, tmp_path):
        result = fb.safe_path("/a/b/c/d/e/f.txt")
        assert result == os.path.join(str(tmp_path), "a", "b", "c", "d", "e", "f.txt")

    def test_leading_slashes(self, tmp_path):
        result = fb.safe_path("///bucket///file")
        assert result == os.path.join(str(tmp_path), "bucket", "file")


# ============================================================
# 2. fmt_size — human-readable file sizes
# ============================================================

class TestFmtSize:
    def test_bytes(self):
        assert fb.fmt_size(0) == "0 B"
        assert fb.fmt_size(512) == "512 B"
        assert fb.fmt_size(1023) == "1023 B"

    def test_kilobytes(self):
        assert fb.fmt_size(1024) == "1.0 KB"
        assert fb.fmt_size(1536) == "1.5 KB"

    def test_megabytes(self):
        assert fb.fmt_size(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        assert fb.fmt_size(1024**3) == "1.0 GB"

    def test_terabytes(self):
        assert fb.fmt_size(1024**4) == "1.0 TB"

    def test_petabytes(self):
        assert fb.fmt_size(1024**5) == "1.0 PB"


# ============================================================
# 3. fmt_time — timestamp formatting
# ============================================================

class TestFmtTime:
    def test_epoch_zero(self):
        assert fb.fmt_time(0) == "1970-01-01 00:00:00 UTC"

    def test_known_timestamp(self):
        # 2025-01-15 12:30:00 UTC = 1736942200 (approx)
        ts = datetime(2025, 1, 15, 12, 30, 0, tzinfo=timezone.utc).timestamp()
        assert fb.fmt_time(ts) == "2025-01-15 12:30:00 UTC"


# ============================================================
# 4. highlight — search term highlighting
# ============================================================

class TestHighlight:
    def test_basic_match(self):
        result = fb.highlight("File992.txt", "992")
        assert "<mark>992</mark>" in result
        assert "File" in result
        assert ".txt" in result

    def test_case_insensitive(self):
        result = fb.highlight("MyFile.TXT", "file")
        assert "<mark>File</mark>" in result

    def test_no_match(self):
        result = fb.highlight("hello.txt", "xyz")
        assert "<mark>" not in result
        assert "hello.txt" in result

    def test_multiple_matches(self):
        result = fb.highlight("abcabcabc", "abc")
        assert result.count("<mark>") == 3

    def test_html_escaping(self):
        result = fb.highlight("<script>alert('xss')</script>", "script")
        assert "<script>" not in result  # escaped
        assert "&lt;" in result
        assert "<mark>script</mark>" in result

    def test_empty_query(self):
        # empty query should return escaped text without marks (fixed: was infinite loop)
        result = fb.highlight("test", "")
        assert result == "test"
        assert "<mark>" not in result


# ============================================================
# 5. render_breadcrumb
# ============================================================

class TestRenderBreadcrumb:
    def test_root(self):
        result = fb.render_breadcrumb("/")
        assert 'href="/"' in result
        assert "Buckets" in result

    def test_bucket_level(self):
        result = fb.render_breadcrumb("/my-bucket/")
        assert "Buckets" in result
        assert "my-bucket" in result
        assert 'href="/my-bucket/"' in result

    def test_nested_path(self):
        result = fb.render_breadcrumb("/bucket/folder/subfolder/")
        assert "Buckets" in result
        assert 'href="/bucket/"' in result
        assert 'href="/bucket/folder/"' in result
        assert "subfolder" in result

    def test_html_escaping_in_path(self):
        result = fb.render_breadcrumb("/my<bucket>/")
        assert "&lt;" in result  # HTML escaped


# ============================================================
# 6. get_mounted_buckets — reads /proc/mounts
# ============================================================

class TestGetMountedBuckets:
    def test_parses_nfs4_mounts(self, tmp_path):
        fb.ROOT_DIR = "/mnt/s3files"
        mock_data = (
            "sysfs /sys sysfs rw 0 0\n"
            "1.2.3.4:/ /mnt/s3files/bucket-a nfs4 rw 0 0\n"
            "5.6.7.8:/ /mnt/s3files/bucket-b nfs4 rw 0 0\n"
            "tmpfs /tmp tmpfs rw 0 0\n"
        )
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_data)):
            buckets = fb.get_mounted_buckets()
        assert len(buckets) == 2
        assert buckets[0]["name"] == "bucket-a"
        assert buckets[1]["name"] == "bucket-b"

    def test_ignores_non_nfs4(self, tmp_path):
        fb.ROOT_DIR = "/mnt/s3files"
        mock_data = "1.2.3.4:/ /mnt/s3files/bucket-a ext4 rw 0 0\n"
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_data)):
            buckets = fb.get_mounted_buckets()
        assert len(buckets) == 0

    def test_ignores_nested_mounts(self, tmp_path):
        fb.ROOT_DIR = "/mnt/s3files"
        mock_data = "1.2.3.4:/ /mnt/s3files/bucket/subfolder nfs4 rw 0 0\n"
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_data)):
            buckets = fb.get_mounted_buckets()
        assert len(buckets) == 0

    def test_handles_missing_proc(self, tmp_path):
        fb.ROOT_DIR = "/mnt/s3files"
        with mock.patch("builtins.open", side_effect=OSError("no such file")):
            buckets = fb.get_mounted_buckets()
        assert buckets == []

    def test_sorted_output(self, tmp_path):
        fb.ROOT_DIR = "/mnt/s3files"
        mock_data = (
            "1.2.3.4:/ /mnt/s3files/zebra nfs4 rw 0 0\n"
            "5.6.7.8:/ /mnt/s3files/alpha nfs4 rw 0 0\n"
        )
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_data)):
            buckets = fb.get_mounted_buckets()
        assert buckets[0]["name"] == "alpha"
        assert buckets[1]["name"] == "zebra"


# ============================================================
# 7. list_dir_sync — paginated directory listing
# ============================================================

class TestListDirSync:
    def test_empty_directory(self, tmp_path):
        result = fb.list_dir_sync(str(tmp_path))
        assert result["dirs"] == []
        assert result["files"] == []
        assert result["page"] == 1
        assert result["total"] == 0

    def test_dirs_and_files(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir))
        dir_names = [d["name"] for d in result["dirs"]]
        file_names = [f["name"] for f in result["files"]]
        assert "alpha" in dir_names
        assert "beta" in dir_names
        assert "file1.txt" in file_names
        assert "file2.log" in file_names

    def test_dirs_sorted_alphabetically(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir))
        dir_names = [d["name"] for d in result["dirs"]]
        assert dir_names == sorted(dir_names)

    def test_files_sorted_alphabetically(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir))
        file_names = [f["name"] for f in result["files"]]
        assert file_names == sorted(file_names)

    def test_dirs_and_files_in_separate_arrays(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir))
        # API returns dirs and files in separate arrays; dirs-first ordering is a JS rendering concern
        assert len(result["dirs"]) == 2   # alpha, beta
        assert len(result["files"]) == 2  # file1.txt, file2.log
        assert all(d["is_dir"] for d in result["dirs"])
        assert not any(f["is_dir"] for f in result["files"])

    def test_item_fields(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir))
        f = result["files"][0]
        assert "name" in f
        assert "is_dir" in f
        assert "size" in f
        assert "mtime" in f
        assert f["is_dir"] is False

        d = result["dirs"][0]
        assert d["is_dir"] is True

    def test_skips_s3files_lost(self, tmp_path):
        (tmp_path / ".s3files-lost+found").mkdir()
        (tmp_path / "visible.txt").write_text("hi")
        result = fb.list_dir_sync(str(tmp_path))
        all_names = [x["name"] for x in result["dirs"] + result["files"]]
        assert ".s3files-lost+found" not in all_names
        assert "visible.txt" in all_names

    def test_pagination_page1(self, large_dir):
        result = fb.list_dir_sync(str(large_dir), page=1)
        assert result["page"] == 1
        total_items = len(result["dirs"]) + len(result["files"])
        assert total_items == fb.PAGE_SIZE
        assert result["has_more"] is True
        assert result["total_pages"] is None  # unknown when has_more

    def test_pagination_page2(self, large_dir):
        result = fb.list_dir_sync(str(large_dir), page=2)
        assert result["page"] == 2
        total_items = len(result["dirs"]) + len(result["files"])
        assert total_items == 50  # PAGE_SIZE+50 total, page2 gets remainder
        assert result["has_more"] is False

    def test_pagination_beyond_range(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir), page=999)
        assert result["dirs"] == []
        assert result["files"] == []
        assert result["page"] == 999

    def test_total_pages_small_dir(self, populated_dir):
        result = fb.list_dir_sync(str(populated_dir), page=1)
        assert result["total_pages"] == 1
        assert result["has_more"] is False

    def test_permission_error(self, tmp_path):
        with mock.patch("os.scandir", side_effect=PermissionError):
            result = fb.list_dir_sync(str(tmp_path))
        assert result["dirs"] == []
        assert result["files"] == []


# ============================================================
# 8. get_bucket_for_mount — mount name → S3 bucket mapping
# ============================================================

class TestGetBucketForMount:
    def test_passthrough_for_normal_buckets(self):
        assert fb.get_bucket_for_mount("my-bucket") == "my-bucket"
        assert fb.get_bucket_for_mount("cloudsee-demo") == "cloudsee-demo"

    def test_caching_prevents_relookup(self):
        """Verify cache is populated and second call doesn't re-execute lookup logic."""
        fb._bucket_name_cache.clear()
        # Pre-seed cache with a mapped value
        fb._bucket_name_cache["cached-bucket"] = "resolved-s3-name"
        # get_bucket_for_mount should return cached value without any external call
        result = fb.get_bucket_for_mount("cached-bucket")
        assert result == "resolved-s3-name"  # proves cache was used, not passthrough

    def test_poc_bucket_boto3_lookup(self):
        mock_cfn = mock.MagicMock()
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"Outputs": [
                {"OutputKey": "BucketName", "OutputValue": "pocs3filesstack-s3filesbucket-abc123"}
            ]}]
        }
        with mock.patch.object(fb, "_s3_client", True), \
             mock.patch("boto3.client", return_value=mock_cfn):
            fb._bucket_name_cache.clear()
            result = fb.get_bucket_for_mount("poc-bucket")
        assert result == "pocs3filesstack-s3filesbucket-abc123"

    def test_poc_bucket_fallback_on_error(self):
        with mock.patch.object(fb, "_s3_client", True), \
             mock.patch("boto3.client", side_effect=Exception("boom")):
            fb._bucket_name_cache.clear()
            result = fb.get_bucket_for_mount("poc-bucket")
        assert result == "poc-bucket"  # falls back to mount name


# ============================================================
# 9. search_s3 — dispatches to boto3 or CLI
# ============================================================

class TestSearchS3:
    def _make_s3_objects(self, keys):
        """Build mock S3 Contents list."""
        return [{"Key": k, "Size": 100, "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc)}
                for k in keys]

    def test_boto3_basic_match(self):
        objects = self._make_s3_objects([
            "folder/File992.txt", "folder/File123.txt", "folder/other992.txt"
        ])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("my-bucket", "992")

        assert scanned == 3
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "File992.txt" in names
        assert "other992.txt" in names

    def test_boto3_case_insensitive(self):
        objects = self._make_s3_objects(["README.md", "readme.bak"])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("bucket", "readme")

        assert len(results) == 2

    def test_boto3_with_prefix(self):
        objects = self._make_s3_objects(["sub/file.txt"])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            fb.search_s3("bucket", "file", prefix="sub/")

        call_kwargs = mock_paginator.paginate.call_args[1]
        assert call_kwargs["Prefix"] == "sub/"

    def test_boto3_caps_at_max_results(self):
        keys = [f"file{i:04d}.txt" for i in range(500)]
        objects = self._make_s3_objects(keys)
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("bucket", "file")

        assert len(results) == fb.MAX_SEARCH_RESULTS

    def test_boto3_result_fields(self):
        objects = self._make_s3_objects(["docs/report.pdf"])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, _ = fb.search_s3("bucket", "report")

        r = results[0]
        assert r["name"] == "report.pdf"
        assert r["key"] == "docs/report.pdf"
        assert r["rel"] == "docs/report.pdf"
        assert r["parent"] == "docs"
        assert r["is_dir"] is False
        assert r["size"] == 100

    def test_boto3_folder_marker(self):
        objects = self._make_s3_objects(["photos/", "photos/cat.jpg"])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, _ = fb.search_s3("bucket", "photos")

        folder_results = [r for r in results if r["is_dir"]]
        assert len(folder_results) == 1
        assert folder_results[0]["name"] == "photos"

    def test_boto3_no_results(self):
        objects = self._make_s3_objects(["file.txt"])
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": objects}]
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("bucket", "nonexistent")

        assert len(results) == 0
        assert scanned == 1

    def test_boto3_empty_bucket(self):
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{}]  # no Contents key
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("bucket", "anything")

        assert results == []
        assert scanned == 0

    def test_boto3_exception_handled(self):
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.side_effect = Exception("network error")
        mock_client = mock.MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with mock.patch.object(fb, "_s3_client", mock_client):
            results, scanned = fb.search_s3("bucket", "test")

        assert results == []
        assert scanned == 0

    def test_cli_fallback_when_no_boto3(self):
        cli_output = json.dumps({
            "Contents": [{"Key": "test.txt", "Size": 50, "LastModified": "2025-01-01T00:00:00Z"}]
        })
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = cli_output

        with mock.patch.object(fb, "_s3_client", None), \
             mock.patch("subprocess.run", return_value=mock_proc):
            results, scanned = fb.search_s3("bucket", "test")

        assert len(results) == 1
        assert results[0]["name"] == "test.txt"

    def test_cli_with_prefix(self):
        cli_output = json.dumps({"Contents": []})
        mock_proc = mock.MagicMock(returncode=0, stdout=cli_output)

        with mock.patch.object(fb, "_s3_client", None), \
             mock.patch("subprocess.run", return_value=mock_proc) as mock_run:
            fb.search_s3("bucket", "test", prefix="folder/")

        cmd = mock_run.call_args[0][0]
        assert "--prefix" in cmd
        assert "folder/" in cmd


# ============================================================
# 10. Render functions — HTML output
# ============================================================

class TestRenderLanding:
    def test_contains_title(self):
        with mock.patch.object(fb, "get_mounted_buckets", return_value=[]):
            html_out = fb.render_landing()
        assert "S3 Files Browser" in html_out
        assert "<!DOCTYPE html>" in html_out

    def test_shows_buckets(self):
        buckets = [{"name": "bucket-a"}, {"name": "bucket-b"}]
        with mock.patch.object(fb, "get_mounted_buckets", return_value=buckets):
            html_out = fb.render_landing()
        assert "bucket-a" in html_out
        assert "bucket-b" in html_out
        assert 'href="/bucket-a/"' in html_out

    def test_no_buckets_message(self):
        with mock.patch.object(fb, "get_mounted_buckets", return_value=[]):
            html_out = fb.render_landing()
        assert "No mounted buckets" in html_out

    def test_success_message(self):
        with mock.patch.object(fb, "get_mounted_buckets", return_value=[]):
            html_out = fb.render_landing(message="Success: uploaded")
        assert "msg-ok" in html_out
        assert "Success: uploaded" in html_out

    def test_error_message(self):
        with mock.patch.object(fb, "get_mounted_buckets", return_value=[]):
            html_out = fb.render_landing(message="Error: something broke")
        assert "msg-err" in html_out

    def test_bucket_count(self):
        buckets = [{"name": f"b{i}"} for i in range(3)]
        with mock.patch.object(fb, "get_mounted_buckets", return_value=buckets):
            html_out = fb.render_landing()
        assert "3 bucket(s)" in html_out


class TestRenderPage:
    def test_contains_breadcrumb(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path))
        assert "mybucket" in html_out
        assert "Buckets" in html_out

    def test_contains_search_bar(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path))
        assert 'name="q"' in html_out
        assert "Search" in html_out

    def test_contains_upload_form(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path))
        assert 'value="upload"' in html_out
        assert 'type="file"' in html_out

    def test_contains_mkdir_form(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path))
        assert 'value="mkdir"' in html_out
        assert "New folder name" in html_out

    def test_contains_async_loader(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path))
        assert "loadPage(1)" in html_out
        assert "/api/ls" in html_out

    def test_shows_message(self, tmp_path):
        html_out = fb.render_page("/mybucket/", str(tmp_path), message="Created folder: test")
        assert "Created folder: test" in html_out
        assert "msg-ok" in html_out

    def test_no_search_bar_at_root(self, tmp_path):
        html_out = fb.render_page("/", str(tmp_path))
        assert 'name="q"' not in html_out


class TestRenderSearchPage:
    def test_contains_spinner(self):
        html_out = fb.render_search_page("/mybucket/", "mybucket", "test")
        assert "spinner" in html_out
        assert "Searching" in html_out

    def test_contains_search_value(self):
        html_out = fb.render_search_page("/mybucket/", "mybucket", "hello")
        assert 'value="hello"' in html_out

    def test_api_search_url(self):
        html_out = fb.render_search_page("/mybucket/", "mybucket", "test")
        assert "/api/search" in html_out

    def test_prefix_scope_label(self):
        html_out = fb.render_search_page("/mybucket/folder/", "mybucket", "test", prefix="folder/")
        assert "folder" in html_out

    def test_html_escapes_query(self):
        html_out = fb.render_search_page("/b/", "b", '<script>alert("xss")</script>')
        # The page has legitimate <script> tags for JS, but user input must be escaped
        # Check the title and search input value are escaped
        assert "&lt;script&gt;alert(" in html_out
        # Ensure user input is not rendered as raw HTML in the search bar value
        assert 'value="&lt;script&gt;' in html_out


# ============================================================
# 11. HTTP Handler — integration tests
# ============================================================

class TestHTTPGetLanding:
    def test_landing_page_200(self, server):
        host, port = server
        status, headers, body = http_get(host, port, "/")
        assert status == 200
        assert "text/html" in headers.get("Content-Type", "")
        assert b"S3 Files Browser" in body

    def test_landing_with_message(self, server):
        host, port = server
        status, _, body = http_get(host, port, "/?msg=Success%3A+done")
        assert status == 200
        assert b"Success: done" in body


class TestHTTPGetApiLs:
    def test_api_ls_empty(self, server, tmp_path):
        host, port = server
        status, headers, body = http_get(host, port, "/api/ls?path=/")
        assert status == 200
        data = json.loads(body)
        assert "dirs" in data
        assert "files" in data
        assert "page" in data
        assert data["page"] == 1

    def test_api_ls_with_files(self, server, tmp_path):
        (tmp_path / "test.txt").write_text("hello")
        (tmp_path / "subdir").mkdir()
        host, port = server
        status, _, body = http_get(host, port, "/api/ls?path=/")
        data = json.loads(body)
        dir_names = [d["name"] for d in data["dirs"]]
        file_names = [f["name"] for f in data["files"]]
        assert "subdir" in dir_names
        assert "test.txt" in file_names

    def test_api_ls_pagination(self, server, tmp_path):
        for i in range(fb.PAGE_SIZE + 10):
            (tmp_path / f"file_{i:05d}.txt").write_text("x")
        host, port = server
        status, _, body = http_get(host, port, "/api/ls?path=/&page=1")
        data = json.loads(body)
        assert len(data["files"]) == fb.PAGE_SIZE
        assert data["has_more"] is True

        status, _, body = http_get(host, port, "/api/ls?path=/&page=2")
        data = json.loads(body)
        assert len(data["files"]) == 10
        assert data["has_more"] is False

    def test_api_ls_404_nonexistent(self, server):
        host, port = server
        status, _, _ = http_get(host, port, "/api/ls?path=/no-such-dir")
        assert status == 404


class TestHTTPGetApiSearch:
    def test_search_empty_params(self, server):
        host, port = server
        status, _, body = http_get(host, port, "/api/search")
        data = json.loads(body)
        assert data["results"] == []
        assert data["capped"] is False

    def test_search_missing_query(self, server):
        host, port = server
        status, _, body = http_get(host, port, "/api/search?bucket=test")
        data = json.loads(body)
        assert data["results"] == []

    def test_search_calls_s3(self, server):
        host, port = server
        mock_results = [{"name": "file.txt", "key": "file.txt", "rel": "file.txt",
                         "parent": "", "is_dir": False, "size": 100, "mtime": "2025-01-01"}]
        with mock.patch.object(fb, "search_s3", return_value=(mock_results, 5)):
            status, _, body = http_get(host, port, "/api/search?bucket=mybucket&q=file")
        data = json.loads(body)
        assert len(data["results"]) == 1
        assert data["scanned"] == 5

    def test_search_with_prefix(self, server):
        host, port = server
        with mock.patch.object(fb, "search_s3", return_value=([], 0)) as mock_search:
            http_get(host, port, "/api/search?bucket=b&q=x&prefix=folder/")
        mock_search.assert_called_once_with("b", "x", "folder/")


class TestHTTPGetDirectoryBrowsing:
    def test_directory_page_200(self, server, tmp_path):
        (tmp_path / "mybucket").mkdir()
        host, port = server
        status, _, body = http_get(host, port, "/mybucket/")
        assert status == 200
        assert b"loadPage(1)" in body

    def test_redirect_missing_slash(self, server, tmp_path):
        (tmp_path / "mybucket").mkdir()
        host, port = server
        conn = HTTPConnection(host, port, timeout=10)
        conn.request("GET", "/mybucket")
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 301
        assert resp.getheader("Location") == "/mybucket/"
        conn.close()

    def test_search_renders_async_page(self, server, tmp_path):
        (tmp_path / "mybucket").mkdir()
        host, port = server
        status, _, body = http_get(host, port, "/mybucket/?q=hello")
        assert status == 200
        assert b"/api/search" in body
        assert b"hello" in body

    def test_search_prefix_extraction(self, server, tmp_path):
        (tmp_path / "mybucket").mkdir()
        (tmp_path / "mybucket" / "subfolder").mkdir()
        host, port = server
        status, _, body = http_get(host, port, "/mybucket/subfolder/?q=test")
        assert status == 200
        # The prefix should be "subfolder/" in the JS
        assert b"subfolder/" in body


class TestHTTPGetFileDownload:
    def test_download_file(self, server, tmp_path):
        (tmp_path / "bucket").mkdir()
        (tmp_path / "bucket" / "hello.txt").write_text("Hello World!")
        host, port = server
        status, headers, body = http_get(host, port, "/bucket/hello.txt")
        assert status == 200
        assert body == b"Hello World!"
        assert "application/octet-stream" in headers.get("Content-Type", "")
        assert 'hello.txt' in headers.get("Content-Disposition", "")

    def test_download_nonexistent_404(self, server, tmp_path):
        host, port = server
        status, _, _ = http_get(host, port, "/no-such-file.txt")
        assert status == 404


class TestHTTPPostMkdir:
    def test_create_folder(self, server, tmp_path):
        (tmp_path / "bucket").mkdir()
        host, port = server
        status, headers, _ = http_post_form(host, port, "/bucket/",
                                             {"action": "mkdir", "dirname": "newfolder"})
        assert status == 303
        assert (tmp_path / "bucket" / "newfolder").is_dir()
        assert "Created" in urllib.parse.unquote(headers.get("Location", ""))

    def test_create_folder_invalid_name(self, server, tmp_path):
        (tmp_path / "bucket").mkdir()
        host, port = server
        status, headers, _ = http_post_form(host, port, "/bucket/",
                                             {"action": "mkdir", "dirname": "bad/name"})
        assert status == 303
        assert not (tmp_path / "bucket" / "bad/name").exists()
        assert "invalid" in urllib.parse.unquote(headers.get("Location", "")).lower()


class TestHTTPPostDelete:
    def test_delete_file(self, server, tmp_path):
        (tmp_path / "bucket").mkdir()
        (tmp_path / "bucket" / "deleteme.txt").write_text("bye")
        host, port = server
        status, _, _ = http_post_form(host, port, "/bucket/deleteme.txt",
                                       {"action": "delete"})
        assert status == 303
        assert not (tmp_path / "bucket" / "deleteme.txt").exists()

    def test_delete_folder(self, server, tmp_path):
        (tmp_path / "bucket").mkdir()
        folder = tmp_path / "bucket" / "rmdir"
        folder.mkdir()
        (folder / "inside.txt").write_text("x")
        host, port = server
        status, _, _ = http_post_form(host, port, "/bucket/rmdir",
                                       {"action": "delete"})
        assert status == 303
        assert not folder.exists()

    def test_delete_mount_refused(self, server, tmp_path):
        mount = tmp_path / "bucket"
        mount.mkdir()
        host, port = server
        with mock.patch("os.path.ismount", return_value=True):
            status, headers, _ = http_post_form(host, port, "/bucket",
                                                 {"action": "delete"})
        assert status == 303
        assert mount.exists()
        assert "cannot delete" in urllib.parse.unquote(headers.get("Location", "")).lower()

    def test_cannot_delete_root(self, server, tmp_path):
        host, port = server
        status, _, _ = http_post_form(host, port, "/", {"action": "delete"})
        assert status == 303
        assert tmp_path.exists()


# ============================================================
# 12. Edge cases
# ============================================================

class TestEdgeCases:
    def test_safe_path_null_bytes(self, tmp_path):
        # Null bytes should not crash
        result = fb.safe_path("/bucket/file\x00.txt")
        assert result.startswith(str(tmp_path))

    def test_fmt_size_negative(self):
        # Negative sizes shouldn't crash
        result = fb.fmt_size(-1)
        assert isinstance(result, str)

    def test_highlight_special_regex_chars(self):
        # Query with regex-special chars should not crash (it's substring, not regex)
        result = fb.highlight("file[1].txt", "[1]")
        assert "<mark>" in result

    def test_breadcrumb_empty(self):
        result = fb.render_breadcrumb("")
        assert "Buckets" in result

    def test_list_dir_page_zero_returns_empty(self, populated_dir):
        """page=0 → start=-PAGE_SIZE, Python slice returns empty. Server clamps to 1."""
        result = fb.list_dir_sync(str(populated_dir), page=0)
        # Negative slice start returns empty in Python
        assert result["dirs"] == []
        assert result["files"] == []
        assert result["page"] == 0

    def test_search_s3_dispatches_boto3_when_available(self):
        """Verify search_s3 calls _search_s3_boto3 (not CLI) when _s3_client is set."""
        with mock.patch.object(fb, "_s3_client", mock.MagicMock()), \
             mock.patch.object(fb, "_search_s3_boto3", return_value=([], 0)) as m_boto3, \
             mock.patch.object(fb, "_search_s3_cli", return_value=([], 0)) as m_cli:
            fb.search_s3("bucket", "query")
        m_boto3.assert_called_once()
        m_cli.assert_not_called()

    def test_search_s3_dispatches_cli_when_no_boto3(self):
        """Verify search_s3 calls _search_s3_cli (not boto3) when _s3_client is None."""
        with mock.patch.object(fb, "_s3_client", None), \
             mock.patch.object(fb, "_search_s3_boto3", return_value=([], 0)) as m_boto3, \
             mock.patch.object(fb, "_search_s3_cli", return_value=([], 0)) as m_cli:
            fb.search_s3("bucket", "query")
        m_cli.assert_called_once()
        m_boto3.assert_not_called()

    def test_concurrent_requests(self, server, tmp_path):
        """Server should handle concurrent requests without deadlock."""
        (tmp_path / "bucket").mkdir()
        for i in range(5):
            (tmp_path / "bucket" / f"file{i}.txt").write_text(f"data{i}")

        host, port = server
        results = [None] * 10
        errors = []

        def fetch(idx):
            try:
                s, _, b = http_get(host, port, "/api/ls?path=/bucket")
                results[idx] = s
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=fetch, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent request errors: {errors}"
        assert all(r == 200 for r in results)
