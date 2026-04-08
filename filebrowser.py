#!/usr/bin/env python3
"""Multi-bucket web file browser for S3 Files mount points. Python stdlib only."""

import cgi
import html
import os
import shutil
import urllib.parse
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

ROOT_DIR = "/mnt/s3files"
PORT = 80

def safe_path(path: str) -> str:
    """Resolve path and ensure it stays within ROOT_DIR."""
    cleaned = os.path.normpath(os.path.join(ROOT_DIR, path.lstrip("/")))
    if not cleaned.startswith(ROOT_DIR):
        return ROOT_DIR
    return cleaned

def fmt_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} PB"

def fmt_time(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def get_mounted_buckets():
    """Discover mounted S3 Files buckets by reading /proc/mounts. Zero NFS calls — instant."""
    buckets = []
    prefix = ROOT_DIR + "/"
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[1].startswith(prefix) and parts[2] == "nfs4":
                    name = parts[1][len(prefix):]
                    if name and "/" not in name:
                        buckets.append({"name": name})
    except OSError:
        pass
    buckets.sort(key=lambda b: b["name"])
    return buckets

PAGE_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
       background: #0f1117; color: #e1e4e8; line-height: 1.5; }
.header { background: linear-gradient(135deg, #1a1f35 0%, #0d1117 100%);
           border-bottom: 1px solid #30363d; padding: 20px 24px; }
.header h1 { font-size: 22px; font-weight: 600; color: #58a6ff; }
.header .subtitle { font-size: 13px; color: #8b949e; margin-top: 4px; }
.container { max-width: 1100px; margin: 0 auto; padding: 24px; }
.breadcrumb { font-size: 14px; margin-bottom: 16px; color: #8b949e; }
.breadcrumb a { color: #58a6ff; text-decoration: none; }
.breadcrumb a:hover { text-decoration: underline; }
.actions { display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
.actions form { display: flex; gap: 8px; align-items: center; }
.btn { background: #21262d; border: 1px solid #30363d; color: #c9d1d9; padding: 6px 14px;
       border-radius: 6px; cursor: pointer; font-size: 13px; text-decoration: none; display: inline-block; }
.btn:hover { background: #30363d; border-color: #8b949e; }
.btn-primary { background: #238636; border-color: #2ea043; color: #fff; }
.btn-primary:hover { background: #2ea043; }
.btn-danger { background: #da3633; border-color: #f85149; color: #fff; }
.btn-danger:hover { background: #b62324; }
input[type=text] { background: #161b22; border: 1px solid #30363d; color: #c9d1d9;
                    padding: 6px 10px; border-radius: 6px; font-size: 13px; }
input[type=file] { color: #8b949e; font-size: 13px; }
table { width: 100%; border-collapse: collapse; background: #161b22;
        border: 1px solid #30363d; border-radius: 6px; overflow: hidden; }
th { background: #1c2128; text-align: left; padding: 10px 16px; font-size: 13px;
     color: #8b949e; font-weight: 600; border-bottom: 1px solid #30363d; }
td { padding: 8px 16px; border-bottom: 1px solid #21262d; font-size: 14px; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #1c2128; }
td a { color: #58a6ff; text-decoration: none; }
td a:hover { text-decoration: underline; }
.icon { margin-right: 6px; }
.folder .icon { color: #54aeff; }
.file .icon { color: #8b949e; }
.size, .modified { color: #8b949e; }
.empty { text-align: center; padding: 48px; color: #484f58; }
.msg { padding: 10px 16px; margin-bottom: 16px; border-radius: 6px; font-size: 14px; }
.msg-ok { background: #0d2818; border: 1px solid #238636; color: #3fb950; }
.msg-err { background: #2d1214; border: 1px solid #da3633; color: #f85149; }
.bucket-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; margin-top: 16px; }
.bucket-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
               padding: 20px; transition: border-color 0.2s, background 0.2s; }
.bucket-card:hover { border-color: #58a6ff; background: #1c2128; }
.bucket-card a { text-decoration: none; color: inherit; display: block; }
.bucket-card .name { font-size: 16px; font-weight: 600; color: #58a6ff; margin-bottom: 8px; }
.bucket-card .meta { font-size: 13px; color: #8b949e; }
.bucket-card .meta span { margin-right: 16px; }
.bucket-icon { font-size: 28px; margin-bottom: 8px; }
.section-title { font-size: 16px; font-weight: 600; color: #c9d1d9; margin-bottom: 4px; }
.section-desc { font-size: 13px; color: #8b949e; margin-bottom: 16px; }
.search-bar { display: flex; gap: 8px; align-items: center; margin-bottom: 16px;
              padding: 12px 16px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; }
.search-bar input[type=text] { flex: 1; padding: 8px 12px; font-size: 14px; min-width: 200px; }
.search-bar .btn { padding: 8px 18px; }
.search-info { font-size: 13px; color: #8b949e; margin-bottom: 12px; }
.search-info strong { color: #c9d1d9; }
.search-info a { color: #58a6ff; text-decoration: none; }
.search-info a:hover { text-decoration: underline; }
mark { background: #2f6b3a; color: #fff; padding: 1px 2px; border-radius: 2px; }
.path-col { font-size: 12px; color: #8b949e; }
.path-col a { color: #8b949e; }
.path-col a:hover { color: #58a6ff; }
.pagination { display: flex; gap: 6px; margin-top: 16px; align-items: center;
              justify-content: center; flex-wrap: wrap; padding: 12px; }
.pagination .btn { min-width: 36px; text-align: center; cursor: pointer; user-select: none; }
.pagination .btn.current { background: #238636; border-color: #2ea043; color: #fff; cursor: default; }
.pagination .btn.disabled { opacity: 0.4; cursor: default; pointer-events: none; }
.pagination .page-info { color: #8b949e; font-size: 13px; margin: 0 8px; }
"""

import subprocess
MAX_SEARCH_RESULTS = 200
MAX_SEARCH_OBJECTS = 200000   # max objects to scan per search

# --- boto3 for fast S3 API access (no subprocess overhead) ---
try:
    import boto3
    _s3_client = boto3.client("s3", region_name="us-east-1")
except ImportError:
    _s3_client = None

# Map mount names to S3 bucket names (mount name = bucket name for existing buckets)
_bucket_name_cache = {}

def get_bucket_for_mount(mount_name: str) -> str:
    """Map mount directory name to actual S3 bucket name. Cached."""
    if mount_name in _bucket_name_cache:
        return _bucket_name_cache[mount_name]
    result = mount_name  # default: mount name = bucket name
    if mount_name == "poc-bucket":
        try:
            if _s3_client:
                import boto3
                cfn = boto3.client("cloudformation", region_name="us-east-1")
                resp = cfn.describe_stacks(StackName="PocS3FilesStack")
                for output in resp["Stacks"][0].get("Outputs", []):
                    if output["OutputKey"] == "BucketName":
                        result = output["OutputValue"]
                        break
            else:
                proc = subprocess.run(
                    ["aws", "cloudformation", "describe-stacks", "--stack-name", "PocS3FilesStack",
                     "--query", "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue",
                     "--output", "text", "--region", "us-east-1"],
                    capture_output=True, text=True, timeout=10)
                if proc.returncode == 0 and proc.stdout.strip():
                    result = proc.stdout.strip()
        except Exception:
            pass
    _bucket_name_cache[mount_name] = result
    return result

def search_s3(bucket_name: str, query: str, prefix: str = "") -> tuple:
    """Search S3 objects by key substring using boto3 paginator (fast — no NFS, no subprocess).
    If prefix is given, only search within that S3 key prefix (folder).
    Returns (results, scanned_count)."""
    query_lower = query.lower()
    results = []
    scanned = 0

    if _s3_client:
        return _search_s3_boto3(bucket_name, query_lower, prefix)
    return _search_s3_cli(bucket_name, query_lower, prefix)

def _search_s3_boto3(bucket_name: str, query_lower: str, prefix: str) -> tuple:
    """Search using boto3 paginator — fastest path."""
    results = []
    scanned = 0
    try:
        paginator = _s3_client.get_paginator("list_objects_v2")
        page_config = {"MaxItems": MAX_SEARCH_OBJECTS, "PageSize": 1000}
        params = {"Bucket": bucket_name, "PaginationConfig": page_config}
        if prefix:
            params["Prefix"] = prefix
        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                scanned += 1
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1] if "/" in key else key
                if not name:
                    name = key.rstrip("/").rsplit("/", 1)[-1] if "/" in key.rstrip("/") else key.rstrip("/")
                if query_lower in name.lower():
                    parent = key[:key.rfind("/")] if "/" in key else ""
                    is_dir = key.endswith("/")
                    results.append({
                        "name": name, "key": key, "rel": key.rstrip("/"),
                        "parent": parent, "is_dir": is_dir,
                        "size": obj.get("Size", 0),
                        "mtime": obj["LastModified"].isoformat() if hasattr(obj.get("LastModified"), "isoformat") else str(obj.get("LastModified", "")),
                    })
                    if len(results) >= MAX_SEARCH_RESULTS:
                        return results, scanned
    except Exception:
        pass
    return results, scanned

def _search_s3_cli(bucket_name: str, query_lower: str, prefix: str) -> tuple:
    """Fallback: search using AWS CLI subprocess."""
    results = []
    scanned = 0
    token = None
    try:
        while len(results) < MAX_SEARCH_RESULTS and scanned < MAX_SEARCH_OBJECTS:
            remaining = min(10000, MAX_SEARCH_OBJECTS - scanned)
            cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket_name,
                   "--page-size", "1000", "--max-items", str(remaining)]
            if prefix:
                cmd += ["--prefix", prefix]
            if token:
                cmd += ["--starting-token", token]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if proc.returncode != 0:
                break
            data = json_mod.loads(proc.stdout)
            contents = data.get("Contents", [])
            scanned += len(contents)
            for obj in contents:
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1] if "/" in key else key
                if not name:
                    name = key.rstrip("/").rsplit("/", 1)[-1] if "/" in key.rstrip("/") else key.rstrip("/")
                if query_lower in name.lower():
                    parent = key[:key.rfind("/")] if "/" in key else ""
                    is_dir = key.endswith("/")
                    results.append({
                        "name": name, "key": key, "rel": key.rstrip("/"),
                        "parent": parent, "is_dir": is_dir,
                        "size": obj.get("Size", 0),
                        "mtime": obj.get("LastModified", ""),
                    })
                    if len(results) >= MAX_SEARCH_RESULTS:
                        break
            token = data.get("NextToken")
            if not token:
                break
    except (subprocess.TimeoutExpired, Exception):
        pass
    return results, scanned

def highlight(text: str, query: str) -> str:
    """Highlight query matches in text (case-insensitive)."""
    lower = text.lower()
    q_lower = query.lower()
    out, i = [], 0
    while i < len(text):
        pos = lower.find(q_lower, i)
        if pos == -1:
            out.append(html.escape(text[i:]))
            break
        out.append(html.escape(text[i:pos]))
        out.append(f"<mark>{html.escape(text[pos:pos+len(query)])}</mark>")
        i = pos + len(query)
    return "".join(out)

def render_breadcrumb(rel_path: str) -> str:
    parts = [p for p in rel_path.split("/") if p]
    crumbs = ['<a href="/">Buckets</a>']
    for i, part in enumerate(parts):
        link = "/" + "/".join(parts[:i+1]) + "/"
        crumbs.append(f'<a href="{html.escape(link)}">{html.escape(part)}</a>')
    return " / ".join(crumbs)

def render_landing(message: str = "") -> str:
    """Render the multi-bucket landing page."""
    buckets = get_mounted_buckets()

    msg_html = ""
    if message:
        cls = "msg-ok" if "success" in message.lower() or "created" in message.lower() else "msg-err"
        msg_html = f'<div class="msg {cls}">{html.escape(message)}</div>'

    cards = []
    for b in buckets:
        cards.append(f'''<div class="bucket-card">
            <a href="/{html.escape(b["name"])}/">
            <div class="bucket-icon">&#128230;</div>
            <div class="name">{html.escape(b["name"])}</div>
            <div class="meta"><span>Mounted via S3 Files</span></div>
            </a></div>''')

    if not cards:
        grid = '<div class="empty">No mounted buckets found</div>'
    else:
        grid = f'<div class="bucket-grid">{"".join(cards)}</div>'

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>S3 Files Browser</title><style>{PAGE_CSS}</style></head><body>
<div class="header"><h1>S3 Files Browser</h1>
<div class="subtitle">Browse S3 buckets mounted via NFS 4.2 &mdash; changes sync to S3 within ~1 minute</div></div>
<div class="container">
{msg_html}
<div class="section-title">Connected Buckets</div>
<div class="section-desc">{len(buckets)} bucket(s) mounted via S3 Files</div>
{grid}
</div></body></html>"""


def render_search_page(rel_path: str, bucket_name: str, query: str, prefix: str = "") -> str:
    """Render async search page - loads instantly with spinner, JS fetches /api/search."""
    action_url = "/" + rel_path.strip("/") + "/"
    action_url = action_url.replace("//", "/")
    search_bar = f'''<div class="search-bar">
        <form method="GET" action="{html.escape(action_url)}" style="display:flex;gap:8px;flex:1;align-items:center">
        <input type="text" name="q" value="{html.escape(query)}" placeholder="Search file names in {html.escape(action_url.strip('/'))}..." style="flex:1">
        <button class="btn" type="submit">Search</button></form></div>'''

    scope_label = f" in <strong>{html.escape(prefix.rstrip('/'))}</strong>" if prefix else ""

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Search: {html.escape(query)} - S3 Files Browser</title><style>{PAGE_CSS}
.spinner {{ display:inline-block;width:20px;height:20px;border:2px solid #30363d;
  border-top-color:#58a6ff;border-radius:50%;animation:spin .8s linear infinite; }}
@keyframes spin {{ to {{ transform:rotate(360deg) }} }}
.loading {{ text-align:center;padding:40px;color:#8b949e; }}
.loading .spinner {{ margin-right: 10px; vertical-align: middle; }}
</style></head><body>
<div class="header"><h1>S3 Files Browser</h1>
<div class="subtitle">Browse S3 buckets mounted via NFS 4.2 &mdash; changes sync to S3 within ~1 minute</div></div>
<div class="container">
<div class="breadcrumb">{render_breadcrumb(rel_path)}</div>
{search_bar}
<div id="results"><div class="loading"><span class="spinner"></span> Searching for &ldquo;{html.escape(query)}&rdquo;{scope_label} via S3 API&hellip;</div></div>
</div>
<script>
const bucketName = {json_mod.dumps(bucket_name)};
const query = {json_mod.dumps(query)};
const prefix = {json_mod.dumps(prefix)};

function esc(s) {{ const d=document.createElement('div');d.textContent=s;return d.innerHTML; }}
function fmtSize(b) {{
  for (const u of ['B','KB','MB','GB','TB']) {{ if(b<1024) return u==='B'?b+' B':b.toFixed(1)+' '+u; b/=1024; }}
  return b.toFixed(1)+' PB';
}}
function highlight(text, q) {{
  const lower = text.toLowerCase(), ql = q.toLowerCase();
  let out = '', i = 0;
  while (i < text.length) {{
    const pos = lower.indexOf(ql, i);
    if (pos === -1) {{ out += esc(text.slice(i)); break; }}
    out += esc(text.slice(i, pos));
    out += '<mark>' + esc(text.slice(pos, pos + q.length)) + '</mark>';
    i = pos + q.length;
  }}
  return out;
}}

fetch('/api/search?bucket='+encodeURIComponent(bucketName)+'&q='+encodeURIComponent(query)+'&prefix='+encodeURIComponent(prefix))
  .then(r=>r.json())
  .then(data=>{{
    const results = data.results;
    const capped = data.capped;
    const clearPath = prefix ? '/'+encodeURIComponent(bucketName)+'/'+prefix.split('/').filter(Boolean).map(encodeURIComponent).join('/')+'/' : '/'+encodeURIComponent(bucketName)+'/';
    const scopeNote = prefix ? ' in <strong>'+esc(prefix)+'</strong>' : '';
    const scannedNote = data.scanned ? ' (scanned '+data.scanned.toLocaleString()+' objects)' : '';
    let info = '<div class="search-info"><strong>'+results.length+(capped?'+':'')+'</strong> result(s) for &ldquo;<strong>'+esc(query)+'</strong>&rdquo;'+scopeNote+scannedNote+' &mdash; <a href="'+clearPath+'">clear search</a></div>';
    if (data.partial) info += '<div class="msg msg-ok" style="margin-top:8px;margin-bottom:12px">Search capped at '+data.scanned.toLocaleString()+' objects. Navigate into a subfolder for more targeted results.</div>';
    let rows = '';
    for (const r of results) {{
      const icon = r.is_dir ? '&#128193;' : '&#128196;';
      const cls = r.is_dir ? 'folder' : 'file';
      const link = '/'+encodeURIComponent(bucketName)+'/'+r.rel.split('/').map(encodeURIComponent).join('/')+(r.is_dir?'/':'');
      const parentLink = r.parent ? '/'+encodeURIComponent(bucketName)+'/'+r.parent.split('/').map(encodeURIComponent).join('/')+'/' : '/'+encodeURIComponent(bucketName)+'/';
      const size = r.is_dir ? '-' : fmtSize(r.size);
      rows += '<tr class="'+cls+'"><td><span class="icon">'+icon+'</span> <a href="'+link+'">'+highlight(r.name,query)+'</a></td>'
        +'<td class="path-col"><a href="'+parentLink+'">'+esc(r.parent||'/')+'</a></td>'
        +'<td class="size">'+size+'</td><td class="modified">'+esc(r.mtime)+'</td></tr>';
    }}
    if (!rows) rows='<tr><td colspan="4" class="empty">No files matching &ldquo;'+esc(query)+'&rdquo;</td></tr>';
    let h = info+'<table><thead><tr><th>Name</th><th>Location</th><th>Size</th><th>Modified</th></tr></thead><tbody>'+rows+'</tbody></table>';
    document.getElementById('results').innerHTML = h;
  }})
  .catch(e=>{{
    document.getElementById('results').innerHTML='<div class="msg msg-err">Search failed: '+esc(e.message)+'</div>';
  }});
</script></body></html>"""


import json as json_mod
import threading
PAGE_SIZE = 200

def list_dir_sync(full_path: str, page: int = 1) -> dict:
    """List directory with pagination. Collects page*PAGE_SIZE entries, sorts, returns the requested page."""
    all_dirs, all_files = [], []
    target = page * PAGE_SIZE
    collected = 0
    has_more = False
    try:
        with os.scandir(full_path) as it:
            for entry in it:
                if entry.name.startswith(".s3files-lost"):
                    continue
                collected += 1
                if collected > target:
                    has_more = True
                    break
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                    st = entry.stat()
                except OSError:
                    continue
                item = {"name": entry.name, "is_dir": is_dir, "size": st.st_size, "mtime": st.st_mtime}
                if is_dir:
                    all_dirs.append(item)
                else:
                    all_files.append(item)
    except PermissionError:
        pass
    all_dirs.sort(key=lambda x: x["name"])
    all_files.sort(key=lambda x: x["name"])
    # Combine sorted dirs-first then files, slice to requested page
    combined = all_dirs + all_files
    start = (page - 1) * PAGE_SIZE
    page_items = combined[start:start + PAGE_SIZE]
    dirs_page = [x for x in page_items if x["is_dir"]]
    files_page = [x for x in page_items if not x["is_dir"]]
    total = len(combined)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE if not has_more else None
    return {
        "dirs": dirs_page, "files": files_page,
        "page": page, "page_size": PAGE_SIZE,
        "total": total, "total_pages": total_pages, "has_more": has_more,
    }

def render_page(rel_path: str, full_path: str, message: str = "") -> str:
    msg_html = ""
    if message:
        cls = "msg-ok" if "success" in message.lower() or "created" in message.lower() else "msg-err"
        msg_html = f'<div class="msg {cls}">{html.escape(message)}</div>'

    action_url = "/" + rel_path.strip("/") + "/"
    action_url = action_url.replace("//", "/")

    parts = [p for p in rel_path.split("/") if p]
    bucket_name = parts[0] if parts else ""
    # Search submits to current directory so prefix is preserved in the URL path
    search_bar = f'''<div class="search-bar">
        <form method="GET" action="{html.escape(action_url)}" style="display:flex;gap:8px;flex:1;align-items:center">
        <input type="text" name="q" placeholder="Search file names in {html.escape(action_url.strip('/'))}..." style="flex:1">
        <button class="btn" type="submit">Search</button></form></div>''' if bucket_name else ""

    parent_link = ""
    if rel_path and rel_path != "/":
        parent = "/" + "/".join(rel_path.strip("/").split("/")[:-1])
        if parent != "/":
            parent += "/"
        parent_link = parent

    # The page loads instantly; JS fetches directory listing async
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>S3 Files Browser</title><style>{PAGE_CSS}
.spinner {{ display:inline-block;width:20px;height:20px;border:2px solid #30363d;
  border-top-color:#58a6ff;border-radius:50%;animation:spin .8s linear infinite; }}
@keyframes spin {{ to {{ transform:rotate(360deg) }} }}
.loading {{ text-align:center;padding:40px;color:#8b949e; }}
.loading .spinner {{ margin-right: 10px; vertical-align: middle; }}
</style></head><body>
<div class="header"><h1>S3 Files Browser</h1>
<div class="subtitle">Browse S3 buckets mounted via NFS 4.2 &mdash; changes sync to S3 within ~1 minute</div></div>
<div class="container">
<div class="breadcrumb">{render_breadcrumb(rel_path)}</div>
{msg_html}
{search_bar}
<div class="actions">
<form method="POST" action="{html.escape(action_url)}" enctype="multipart/form-data">
<input type="hidden" name="action" value="upload">
<input type="file" name="file" required multiple>
<button class="btn btn-primary" type="submit">Upload</button></form>
<form method="POST" action="{html.escape(action_url)}">
<input type="hidden" name="action" value="mkdir">
<input type="text" name="dirname" placeholder="New folder name" required>
<button class="btn" type="submit">Create Folder</button></form>
</div>
<div id="listing"><div class="loading"><span class="spinner"></span> Loading directory contents&hellip;</div></div>
</div>
<script>
const relPath = {json_mod.dumps(rel_path)};
const parentLink = {json_mod.dumps(parent_link)};
const actionUrl = {json_mod.dumps(action_url)};

function esc(s) {{ const d=document.createElement('div');d.textContent=s;return d.innerHTML; }}
function fmtSize(b) {{
  for (const u of ['B','KB','MB','GB','TB']) {{ if(b<1024) return u==='B'?b+' B':b.toFixed(1)+' '+u; b/=1024; }}
  return b.toFixed(1)+' PB';
}}
function fmtTime(ts) {{
  const d=new Date(ts*1000);
  return d.toISOString().replace('T',' ').replace(/\\.\\d+Z/,' UTC');
}}

function loadPage(page) {{
  document.getElementById('listing').innerHTML='<div class="loading"><span class="spinner"></span> Loading page '+page+'&hellip;</div>';
  fetch('/api/ls?path='+encodeURIComponent(relPath)+'&page='+page)
    .then(r=>r.json())
    .then(data=>{{
      let rows='';
      if(parentLink && page===1) rows+='<tr class="folder"><td><span class="icon">&#128193;</span> <a href="'+esc(parentLink)+'">..</a></td><td></td><td></td><td></td></tr>';
      for(const d of data.dirs) {{
        const link=actionUrl+encodeURIComponent(d.name)+'/';
        rows+='<tr class="folder"><td><span class="icon">&#128193;</span> <a href="'+link+'">'+esc(d.name)+'/</a></td>'
          +'<td class="size">-</td><td class="modified">'+fmtTime(d.mtime)+'</td>'
          +'<td><form method="POST" action="'+link+'" onsubmit="return confirm(\\'Delete folder?\\')"><input type="hidden" name="action" value="delete"><button class="btn btn-danger" type="submit">Delete</button></form></td></tr>';
      }}
      for(const f of data.files) {{
        const link=actionUrl+encodeURIComponent(f.name);
        rows+='<tr class="file"><td><span class="icon">&#128196;</span> <a href="'+link+'">'+esc(f.name)+'</a></td>'
          +'<td class="size">'+fmtSize(f.size)+'</td><td class="modified">'+fmtTime(f.mtime)+'</td>'
          +'<td><form method="POST" action="'+link+'" onsubmit="return confirm(\\'Delete file?\\')"><input type="hidden" name="action" value="delete"><button class="btn btn-danger" type="submit">Delete</button></form></td></tr>';
      }}
      if(!rows) rows='<tr><td colspan="4" class="empty">Empty directory</td></tr>';
      let h='<table><thead><tr><th>Name</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead><tbody>'+rows+'</tbody></table>';

      // Pagination controls
      const pg = data.page;
      const totalPages = data.total_pages;
      const hasMore = data.has_more;
      const showPag = totalPages === null ? hasMore : totalPages > 1;
      if (showPag) {{
        h += '<div class="pagination">';
        h += '<span class="btn'+(pg<=1?' disabled':'')+'" onclick="'+(pg>1?'loadPage('+(pg-1)+')':'')+'">&laquo; Prev</span>';
        // Page window: show first, last-known, and pages around current
        const knownLast = totalPages || pg + 1;
        const pages = new Set();
        pages.add(1);
        for (let i = Math.max(2, pg-2); i <= Math.min(knownLast, pg+2); i++) pages.add(i);
        if (totalPages) pages.add(totalPages);
        const sorted = [...pages].sort((a,b)=>a-b);
        let prev = 0;
        for (const p of sorted) {{
          if (p - prev > 1) h += '<span class="page-info">&hellip;</span>';
          h += '<span class="btn'+(p===pg?' current':'')+'" onclick="'+(p!==pg?'loadPage('+p+')':'')+'">'+p+'</span>';
          prev = p;
        }}
        if (hasMore && !totalPages) h += '<span class="page-info">&hellip;</span>';
        const canNext = hasMore || (totalPages && pg < totalPages);
        h += '<span class="btn'+(canNext?'':' disabled')+'" onclick="'+(canNext?'loadPage('+(pg+1)+')':'')+'">Next &raquo;</span>';
        h += '<span class="page-info">'+data.total.toLocaleString()+(hasMore?'+':'')+' items</span>';
        h += '</div>';
      }}
      document.getElementById('listing').innerHTML=h;
      window.scrollTo(0,0);
    }})
    .catch(e=>{{
      document.getElementById('listing').innerHTML='<div class="msg msg-err">Failed to load: '+esc(e.message)+'</div>';
    }});
}}
loadPage(1);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        qs = urllib.parse.parse_qs(parsed.query)
        msg = qs.get("msg", [""])[0]

        # Landing page
        if path == "/" or path == "":
            body = render_landing(message=msg).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # API endpoint for async directory listing
        if path == "/api/ls":
            ls_path = qs.get("path", [""])[0]
            page = max(1, int(qs.get("page", ["1"])[0]))
            full = safe_path(ls_path)
            if os.path.isdir(full):
                data = list_dir_sync(full, page=page)
                body = json_mod.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)
            return

        # API endpoint for async search via S3 API
        if path == "/api/search":
            bucket_mount = qs.get("bucket", [""])[0]
            q = qs.get("q", [""])[0].strip()
            prefix = qs.get("prefix", [""])[0]
            if bucket_mount and q:
                s3_bucket = get_bucket_for_mount(bucket_mount)
                results, scanned = search_s3(s3_bucket, q, prefix)
                resp = {
                    "results": results,
                    "capped": len(results) >= MAX_SEARCH_RESULTS,
                    "scanned": scanned,
                    "partial": scanned >= MAX_SEARCH_OBJECTS,
                }
            else:
                resp = {"results": [], "capped": False, "scanned": 0, "partial": False}
            body = json_mod.dumps(resp).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        full = safe_path(path)
        query = qs.get("q", [""])[0].strip()

        if os.path.isdir(full):
            if not path.endswith("/") and not query:
                self.send_response(301)
                self.send_header("Location", path + "/")
                self.end_headers()
                return

            # Search mode: ?q=term (async via S3 API with prefix scoping)
            if query:
                parts = [p for p in path.split("/") if p]
                bucket_name = parts[0] if parts else ""
                # Everything after bucket name is the S3 key prefix (folder scope)
                prefix = "/".join(parts[1:])
                if prefix and not prefix.endswith("/"):
                    prefix += "/"
                body = render_search_page(path, bucket_name, query, prefix).encode()
            else:
                body = render_page(path, full, message=msg).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif os.path.isfile(full):
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{os.path.basename(full)}"')
            self.send_header("Content-Length", str(os.path.getsize(full)))
            self.end_headers()
            with open(full, "rb") as f:
                shutil.copyfileobj(f, self.wfile)
        else:
            self.send_error(404)

    def do_POST(self):
        path = urllib.parse.unquote(urllib.parse.urlparse(self.path).path)
        content_type = self.headers.get("Content-Type", "")
        content_length = int(self.headers.get("Content-Length", 0))

        if "multipart/form-data" in content_type:
            form = cgi.FieldStorage(
                fp=self.rfile, headers=self.headers,
                environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type})
            action = form.getfirst("action", "")
        else:
            body = self.rfile.read(content_length).decode()
            params = urllib.parse.parse_qs(body)
            action = params.get("action", [""])[0]
            form = None

        msg = ""
        redirect = path if path.endswith("/") else os.path.dirname(path) + "/"

        if action == "upload" and form:
            files = form["file"] if isinstance(form["file"], list) else [form["file"]]
            dir_path = safe_path(path)
            count = 0
            for item in files:
                if item.filename:
                    fname = os.path.basename(item.filename)
                    dest = os.path.join(dir_path, fname)
                    with open(dest, "wb") as out:
                        shutil.copyfileobj(item.file, out)
                    count += 1
            msg = f"Success: uploaded {count} file(s)"

        elif action == "mkdir":
            dirname = params.get("dirname", [""])[0].strip()
            if dirname and "/" not in dirname:
                target = os.path.join(safe_path(path), dirname)
                os.makedirs(target, exist_ok=True)
                msg = f"Created folder: {dirname}"
            else:
                msg = "Error: invalid folder name"

        elif action == "delete":
            target = safe_path(path)
            if target != ROOT_DIR and target.startswith(ROOT_DIR):
                name = os.path.basename(target)
                if os.path.isdir(target) and not os.path.ismount(target):
                    shutil.rmtree(target)
                    msg = f"Deleted folder: {name}"
                elif os.path.isfile(target):
                    os.remove(target)
                    msg = f"Deleted file: {name}"
                elif os.path.ismount(target):
                    msg = "Error: cannot delete a mounted bucket"
                redirect = os.path.dirname(target.rstrip("/"))
                redirect = "/" + os.path.relpath(redirect, ROOT_DIR) + "/"
                if redirect == "/./" or redirect == "/.":
                    redirect = "/"

        sep = "?" if "?" not in redirect else "&"
        location = redirect + (f"{sep}msg={urllib.parse.quote(msg)}" if msg else "")
        self.send_response(303)
        self.send_header("Location", location)
        self.end_headers()

    def log_message(self, format, *args):
        pass


from socketserver import ThreadingMixIn

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

if __name__ == "__main__":
    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"S3 Files Browser running on http://0.0.0.0:{PORT}")
    server.serve_forever()
