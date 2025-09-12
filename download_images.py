#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import argparse
import unicodedata
import threading
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import requests

# --- Pillow for WebP conversion ---
# Installs: pip install pillow
try:
    from PIL import Image
except ImportError:
    raise SystemExit("Pillow is required. Install with: pip install pillow")

# --- tqdm (optional progress bar with ETA) ---
# Installs: pip install tqdm
try:
    from tqdm.auto import tqdm
    TQDM_AVAILABLE = True
except Exception:
    tqdm = None
    TQDM_AVAILABLE = False


# ------------------------- Path utils -------------------------
def get_script_dir():
    # If run as a script, use its directory; otherwise use current working directory
    return os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()


# ------------------------- CLI -------------------------
# Downloads exactly one image per product, converts to WebP <=50KB, parallelized with caching.
# You can filter by --item or --id, tune concurrency/rate, and choose fast/ultra strategies.
parser = argparse.ArgumentParser(
    description="Download 1 image per product, save as WebP (≤50KB) — parallel, fast/ultra, caching"
)
# filter by item (multiple via repeated flags or comma-separated)
parser.add_argument("--item", action="append", help="Process only given item_id/sku (repeatable or comma-separated)")
parser.add_argument("--id", help="Process only given product id")
parser.add_argument("--out", default=None, help="Output dir. Default: <script_dir>/images")
parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded (no file writes)")

# speed / parallelism
parser.add_argument("--concurrency", type=int, default=8, help="Number of threads (default 8)")
parser.add_argument("--rps", type=float, help="Rate limit: requests per second. 0/omitted = no throttle")
parser.add_argument("--delay", type=float, help="Sleep between requests (sec). Alternative to --rps")

# performance modes
parser.add_argument("--fast", action="store_true", help="Faster: fewer paths, short timeouts, URL cache")
parser.add_argument("--ultra", action="store_true", help="Max speed: minimal paths/timeouts (may miss rare paths)")
parser.add_argument("--no-head", action="store_true", help="Skip HEAD — go straight to GET")
parser.add_argument("--max-candidates", type=int, help="Limit URL candidates to check (ultra default was 2)")

# misc
parser.add_argument("--max", type=int, help="Max products to process (quick test)")
parser.add_argument("--progress", choices=["auto", "tqdm", "simple", "none"], default="auto", help="Progress style")
parser.add_argument("--no-log-failed", action="store_true", help="Do not save missing_urls.log (slightly faster)")
args = parser.parse_args()


# ------------------------- DB config -------------------------
# Read from environment so Docker/Compose can inject credentials.
DB = {
    "host": os.environ.get("DB_HOST", "MySQL-8.0"),
    "user": os.environ.get("DB_USERNAME") or os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "root"),
    "database": os.environ.get("DB_DATABASE") or os.environ.get("DB_NAME", "domus_LOCAL"),
    "port": int(os.environ.get("DB_PORT", "3306")),
}
TABLE_PRODUCTS = os.environ.get("DB_TABLE_PRODUCTS", "products")

print(f"[DB] host={DB['host']} port={DB['port']} db={DB['database']} user={DB['user']}")

# ------------------------- Output folder -------------------------
# All images will be placed under BASE_SAVE_DIR/<item_id>/<basename>.webp
BASE_SAVE_DIR = os.path.join(get_script_dir(), "images") if args.out is None else args.out
os.makedirs(BASE_SAVE_DIR, exist_ok=True)
print(f"[INIT] Base save dir: {os.path.abspath(BASE_SAVE_DIR)}")

# ------------------------- Sources & subfolders -------------------------
# Candidate bases to probe. The code will try multiple path shapes and subfolders.
BASE_URLS = [
    "https://domus.am/storage/media/",
    "https://ik.imagekit.io/yhosuhnly/domus/media/",
]
# Exhaustive subfolders used to discover a valid image URL. fast/ultra reduce this list.
FULL_SUBFOLDERS  = ["", "original", "large", "lg", "xl", "big", "medium", "md", "small", "sm", "thumb", "thumbs"]
FAST_SUBFOLDERS  = ["", "original"]
ULTRA_SUBFOLDERS = ["", "original"]

if args.ultra:
    SUBFOLDERS = ULTRA_SUBFOLDERS
elif args.fast:
    SUBFOLDERS = FAST_SUBFOLDERS
else:
    SUBFOLDERS = FULL_SUBFOLDERS

# ------------------------- Limits & flags -------------------------
TARGET_BYTES = 50 * 1024  # Target size for WebP files (50KB)
LOG_FAILED = (not args.no_log_failed)

ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tif", ".tiff", ".bmp"}
ALLOWED_CT_PREFIX = "image/"

# Timeouts/retries tuned per mode (default/fast/ultra).
# Ultra is fastest but may skip rare path variants.
if args.ultra:
    HEAD_TIMEOUT, GET_TIMEOUT, HEAD_RETRIES, MAX_CANDIDATES = 0.6, 1.8, 0, 2
elif args.fast:
    HEAD_TIMEOUT, GET_TIMEOUT, HEAD_RETRIES, MAX_CANDIDATES = 1.5, 4.0, 0, 6
else:
    HEAD_TIMEOUT, GET_TIMEOUT, HEAD_RETRIES, MAX_CANDIDATES = 6.0, 15.0, 1, None

# Allow overriding candidate limit from CLI
if args.max_candidates is not None:
    MAX_CANDIDATES = args.max_candidates

# ------------------------- Rate limiting -------------------------
# Either --delay or --rps can throttle requests. By default no delay (fastest).
if args.delay is not None and args.rps is not None:
    print("[WARN] Both --delay and --rps specified. Using --delay.")
if args.delay is not None:
    REQUEST_DELAY = max(0.0, args.delay)
elif args.rps is not None:
    REQUEST_DELAY = 0.0 if args.rps <= 0 else 1.0 / float(args.rps)
else:
    REQUEST_DELAY = 0.0  # fastest

# ------------------------- Progress -------------------------
# 'auto' uses tqdm if available, else simple console. 'none' disables progress.
if args.progress == "tqdm" and not TQDM_AVAILABLE:
    print("[WARN] tqdm selected but not installed. Falling back to simple.")
use_tqdm = (args.progress == "tqdm" and TQDM_AVAILABLE) or (args.progress == "auto" and TQDM_AVAILABLE)
use_simple = (args.progress == "simple") or (args.progress == "auto" and not TQDM_AVAILABLE)
use_none = (args.progress == "none")


def log(msg: str):
    """Emit messages that play nice with tqdm or plain stdout."""
    if use_tqdm and 'tqdm' in globals() and tqdm is not None:
        tqdm.write(msg)
    else:
        print(msg)


# ------------------------- Helpers -------------------------
def is_valid_url(url: str) -> bool:
    try:
        r = urlparse(url)
        return all([r.scheme, r.netloc])
    except Exception:
        return False


def _strip_query_fragment(p: str) -> str:
    p = p.split("?", 1)[0]
    p = p.split("#", 1)[0]
    return p


def has_allowed_ext(name_or_url_path: str) -> bool:
    path = _strip_query_fragment(name_or_url_path)
    ext = os.path.splitext(path)[1].lower()
    return ext in ALLOWED_EXTS


def sanitize_filename(filename: str) -> str:
    normalized = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^\w\-_\. ]', '_', normalized)


def dedupe_preserve_order(seq):
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out


# ------------------------- HTTP: thread-local Session + throttle -------------------------
_thread = threading.local()

def get_session():
    if getattr(_thread, "session", None) is None:
        _thread.session = requests.Session()
    return _thread.session

def _throttle():
    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)

def rl_head(url, headers, timeout=None, allow_redirects=True):
    _throttle()
    return get_session().head(url, headers=headers, timeout=(timeout or HEAD_TIMEOUT), allow_redirects=allow_redirects)

def rl_get(url, headers, timeout=None, stream=False):
    _throttle()
    return get_session().get(url, headers=headers, timeout=(timeout or GET_TIMEOUT), stream=stream)


# ------------------------- Candidate URL builder -------------------------
def build_candidate_urls(base_urls, mid: str | None, file_name: str, subfolders=None) -> list[str]:
    subfolders = SUBFOLDERS if subfolders is None else subfolders
    # try raw and quoted names (safe char sets tuned)
    fns = [file_name, quote(file_name, safe="/()._- "), quote(file_name, safe="/()._-")]
    candidates = []
    for base in base_urls:
        base = base.rstrip('/')
        for fn in fns:
            if mid and has_allowed_ext(fn):
                candidates.append(f"{base}/{mid}/{fn}")
                for sf in subfolders:
                    if sf:
                        candidates.append(f"{base}/{mid}/{sf}/{fn}")
            if has_allowed_ext(fn):
                candidates.append(f"{base}/{fn}")
                for sf in subfolders:
                    if sf:
                        candidates.append(f"{base}/{sf}/{fn}")
    return dedupe_preserve_order(candidates)


# ------------------------- URL liveness cache -------------------------
URL_ALIVE_CACHE = {}   # url -> True/False
URL_ALIVE_LOCK = threading.Lock()

def cache_get(url):
    with URL_ALIVE_LOCK:
        return URL_ALIVE_CACHE.get(url)

def cache_set(url, ok):
    with URL_ALIVE_LOCK:
        URL_ALIVE_CACHE[url] = ok


# ------------------------- URL liveness picker -------------------------
def pick_first_alive_url(candidates, timeout=None, debug_list=None, retries=None, max_candidates=None):
    """
    Iterate over candidate URLs and return the first that:
      - responds 200 and Content-Type starts with image/, OR
      - has an allowed image extension (fallback check)
    Uses HEAD by default (or GET with --no-head), honors retries, and caches results.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    retries = HEAD_RETRIES if retries is None else retries
    max_candidates = MAX_CANDIDATES if max_candidates is None else max_candidates

    tried = 0
    for u in candidates:
        if not is_valid_url(u):
            continue
        if max_candidates is not None and tried >= max_candidates:
            break
        tried += 1

        cached = cache_get(u)
        if cached is False:
            continue
        if cached is True:
            return u

        ok = False
        try:
            if args.no_head:
                # GET directly (heavier for the server but simple).
                r = rl_get(u, headers=headers, timeout=timeout, stream=True)
                if r.status_code == 200:
                    ct = r.headers.get("Content-Type", "").lower()
                    if ct.startswith(ALLOWED_CT_PREFIX) or has_allowed_ext(urlparse(u).path):
                        ok = True
                else:
                    if debug_list is not None:
                        debug_list.append(f"{u} -> GET {r.status_code}")
            else:
                attempt = 0
                while attempt <= retries:
                    r = rl_head(u, headers=headers, timeout=timeout, allow_redirects=True)
                    if r.status_code == 200:
                        ct = r.headers.get("Content-Type", "").lower()
                        if ct.startswith(ALLOWED_CT_PREFIX) or has_allowed_ext(urlparse(u).path):
                            ok = True
                            break
                        else:
                            if debug_list is not None:
                                debug_list.append(f"{u} -> 200 but non-image CT: {ct}")
                            break
                    if r.status_code in (403, 405):
                        rg = rl_get(u, headers=headers, timeout=timeout, stream=True)
                        if rg.status_code == 200:
                            ct = rg.headers.get("Content-Type", "").lower()
                            if ct.startswith(ALLOWED_CT_PREFIX) or has_allowed_ext(urlparse(u).path):
                                ok = True
                                break
                            else:
                                if debug_list is not None:
                                    debug_list.append(f"{u} -> 200(GET) but non-image CT: {ct}")
                    else:
                        if debug_list is not None:
                            debug_list.append(f"{u} -> HEAD {r.status_code}")
                    attempt += 1
        except Exception as e:
            if debug_list is not None:
                debug_list.append(f"{u} -> EXC {e.__class__.__name__}")

        cache_set(u, ok)
        if ok:
            return u
    return None


# ------------------------- Media hub lookup -------------------------
def fetch_media_mapping_chunked(cursor, media_ids: list[str], chunk_size: int = 900) -> dict:
    """
    Map media IDs -> file_name from media_hub in chunks to avoid huge IN() lists.
    """
    result = {}
    ids = list(media_ids)
    for i in range(0, len(ids), chunk_size):
        ch = ids[i:i+chunk_size]
        if not ch:
            continue
        placeholders = ",".join(["%s"] * len(ch))
        q = f"SELECT id, file_name FROM media_hub WHERE id IN ({placeholders})"
        cursor.execute(q, tuple(ch))
        for mid, fname in cursor.fetchall():
            result[str(mid)] = fname
    return result


# ------------------------- Column discovery -------------------------
def get_table_columns(cursor, table_name: str) -> list[tuple[str, str]]:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    return [(row[0], row[1]) for row in cursor.fetchall()]

def find_col(candidates: list[str], cols_lower: set, case_map: dict) -> str | None:
    for c in candidates:
        if c in cols_lower:
            return case_map[c]
    return None


# ------------------------- WebP compression -------------------------
def webp_fit_bytes(input_path: str, output_path: str, target_bytes: int = TARGET_BYTES) -> tuple[bool, int]:
    """
    Convert to WebP and try to fit under target_bytes:
      1) Binary search on quality at original size
      2) If still large, iterative downscale (high-quality resampling) + retry
      3) Last resort: save with quality=10
    Returns (ok_fit, final_size_bytes)
    """
    try:
        with Image.open(input_path) as im:
            if getattr(im, "n_frames", 1) > 1:
                im.seek(0)
            has_alpha = (im.mode in ("RGBA", "LA")) or ("transparency" in im.info)
            img = im.convert("RGBA" if has_alpha else "RGB")

            from io import BytesIO

            def try_quality(img, q) -> int:
                bio = BytesIO()
                img.save(bio, format="WEBP", quality=q, method=6)
                return bio.tell()

            def save_webp(img, q, path):
                img.save(path, format="WEBP", quality=q, method=6)

            def fit_quality(img) -> tuple[bool, int]:
                lo, hi = 10, 95
                s_hi = try_quality(img, hi)
                if s_hi <= target_bytes:
                    save_webp(img, hi, output_path)
                    return True, s_hi
                s_lo = try_quality(img, lo)
                if s_lo > target_bytes:
                    return False, s_lo
                best_q, best_size = None, None
                while lo <= hi:
                    mid = (lo + hi) // 2
                    s_mid = try_quality(img, mid)
                    if s_mid <= target_bytes:
                        best_q, best_size = mid, s_mid
                        lo = mid + 1
                    else:
                        hi = mid - 1
                if best_q is not None:
                    save_webp(img, best_q, output_path)
                    return True, best_size
                return False, s_hi

            ok, size_b = fit_quality(img)
            if ok:
                return True, size_b

            # Progressive downscale pass
            max_steps = 6
            scale = 0.85
            w, h = img.size
            for _ in range(max_steps):
                w = max(64, int(w * scale))
                h = max(64, int(h * scale))
                img2 = img.resize((w, h), Image.LANCZOS)
                ok, size_b = fit_quality(img2)
                if ok:
                    return True, size_b

            # Last resort
            img2.save(output_path, format="WEBP", quality=10, method=6)
            return False, os.path.getsize(output_path)

    except Exception as e:
        print(f"[webp] Error: {e}")
        return (False, 0)


# ------------------------- Simple progress printer -------------------------
def print_simple_progress(prefix, i, n, ok, skipped, failed):
    line = f"[{prefix}] {i}/{n} | OK:{ok} SKIP:{skipped} ERR:{failed}"
    endc = "\r" if i < n else "\n"
    print(line, end=endc, flush=True)


# ========================= SELECT & DOWNLOAD WORKERS =========================
def select_one(item_id, product_media_map, product_alt_url_map, media_id_to_file, failed_log_lines):
    """
    For a given item_id, try:
      1) media_hub IDs -> candidate URLs -> pick first alive
      2) fallback alternate URL/path columns -> pick first alive
    Returns (item_id, chosen_url, base_filename_no_ext, error_or_None)
    """
    debug_urls = []
    chosen_url, chosen_fname = None, None

    # collect all (prod_id,item_id) keys with this item_id
    related = [k for k in product_media_map.keys() if k[1] == item_id]

    for key in related:
        media_ids = product_media_map[key]

        # 1) media_hub — prefilter by extension, then probe
        for mid in media_ids:
            file_name = media_id_to_file.get(mid)
            if not file_name:
                if LOG_FAILED:
                    failed_log_lines.append(f"[{item_id}] media id {mid} not found in media_hub")
                continue
            if not has_allowed_ext(file_name):
                continue

            candidates = build_candidate_urls(BASE_URLS, mid, file_name)
            alive = pick_first_alive_url(candidates, debug_list=debug_urls if LOG_FAILED else None)
            if alive:
                chosen_url = alive
                chosen_fname = file_name
                break
        if chosen_url:
            break

        # 2) alternate URL/path
        alt = product_alt_url_map.get(key)
        if alt and not chosen_url:
            if is_valid_url(alt):
                test = pick_first_alive_url([alt], debug_list=debug_urls if LOG_FAILED else None)
                if test:
                    chosen_url = test
                    chosen_fname = os.path.basename(_strip_query_fragment(urlparse(test).path)) or "image"
            else:
                parts = alt.strip("/").split("/")
                mid_guess = parts[0] if parts and parts[0].isdigit() else None
                file_guess = parts[-1] if parts else "image"
                if has_allowed_ext(file_guess):
                    candidates = build_candidate_urls(BASE_URLS, mid_guess, file_guess)
                    alive = pick_first_alive_url(candidates, debug_list=debug_urls if LOG_FAILED else None)
                    if alive:
                        chosen_url = alive
                        chosen_fname = file_guess

    if chosen_url and chosen_fname:
        base_no_ext = os.path.splitext(sanitize_filename(chosen_fname))[0] or "image"
        return (item_id, chosen_url, base_no_ext, None)
    else:
        if LOG_FAILED and debug_urls:
            failed_log_lines.append(f"[{item_id}] tried:\n" + "\n".join(f"  {u}" for u in debug_urls))
        return (item_id, None, None, "no alive image")


def download_one(item_id, url, base_name, dry_run=False):
    """
    Download the chosen URL, verify it's an image, convert to WebP ≤ TARGET_BYTES,
    and write to <BASE_SAVE_DIR>/<item_id>/<base_name>.webp
    """
    item_id_safe = sanitize_filename(item_id)
    save_dir = os.path.join(BASE_SAVE_DIR, item_id_safe)
    os.makedirs(save_dir, exist_ok=True)

    tmp_path = os.path.join(save_dir, f"{base_name}.__tmp__")
    final_path = os.path.join(save_dir, f"{base_name}.webp")

    if dry_run:
        return ("skip", final_path)

    if os.path.exists(final_path):
        return ("skip", final_path)

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = rl_get(url, headers=headers, timeout=GET_TIMEOUT, stream=True)
        r.raise_for_status()

        ct = r.headers.get("Content-Type", "").lower()
        path_ext_ok = has_allowed_ext(urlparse(url).path)
        if not (ct.startswith(ALLOWED_CT_PREFIX) or path_ext_ok):
            return ("err", "not image")

        with open(tmp_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*64):
                if chunk:
                    f.write(chunk)

        # Verify it's actually an image
        try:
            with Image.open(tmp_path) as _probe:
                _probe.verify()
        except Exception:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            return ("err", "broken image")

        ok, _ = webp_fit_bytes(tmp_path, final_path, target_bytes=TARGET_BYTES)
        try:
            os.remove(tmp_path)
        except Exception:
            pass

        return ("ok", final_path)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return ("err", str(e))


# ================================ MAIN =================================
try:
    conn = mysql.connector.connect(**DB)
    cursor = conn.cursor()

    # Discover columns
    cols = get_table_columns(cursor, TABLE_PRODUCTS)
    if not cols:
        print(f"Table `{TABLE_PRODUCTS}` is empty or does not exist.")
        raise SystemExit
    cols_lower = {c.lower() for c, _ in cols}
    case_map = {c.lower(): c for c, _ in cols}

    ID_COL       = find_col(["id", "product_id"], cols_lower, case_map)
    ITEM_ID_COL  = find_col(["item_id", "sku", "code", "product_code", "external_id", "vendor_code", "article", "artikul", "articul", "slug"], cols_lower, case_map)
    MEDIA_COL    = find_col(["media", "media_json", "images", "image_ids", "gallery", "photos", "photo_ids"], cols_lower, case_map)
    ALT_URL_COLS = [c for c in [find_col(["image", "photo", "main_image", "cover", "picture", "img", "preview"], cols_lower, case_map)] if c]

    if not ID_COL:
        print(f"Could not find ID column in `{TABLE_PRODUCTS}`. Available: {[c for c,_ in cols]}")
        raise SystemExit

    ITEM_ID_EXPR = f"`{ITEM_ID_COL}`" if ITEM_ID_COL else f"`{ID_COL}` AS item_id"
    if not MEDIA_COL and not ALT_URL_COLS:
        print("No media_id column and no alternative URL columns (image/photo/...).")
        print(f"Columns: {[c for c,_ in cols]}")
        raise SystemExit

    select_cols = [f"`{ID_COL}`", ITEM_ID_EXPR]
    if MEDIA_COL:
        select_cols.append(f"`{MEDIA_COL}`")
    for c in ALT_URL_COLS:
        select_cols.append(f"`{c}`")

    # --- WHERE filters ---
    where_clauses, params = [], []

    # parse multiple --item (including comma/space separated)
    item_filters = []
    if args.item:
        for raw in args.item:
            parts = re.split(r"[,\s]+", str(raw))
            for p in parts:
                p = p.strip()
                if p:
                    item_filters.append(p)
        item_filters = dedupe_preserve_order(item_filters)

    if item_filters:
        col = ITEM_ID_COL if ITEM_ID_COL else ID_COL
        placeholders = ",".join(["%s"] * len(item_filters))
        where_clauses.append(f"`{col}` IN ({placeholders})")
        params.extend(item_filters)

    if args.id:
        where_clauses.append(f"`{ID_COL}` = %s"); params.append(args.id)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    query = f"SELECT {', '.join(select_cols)} FROM `{TABLE_PRODUCTS}`{where_sql} ORDER BY `{ID_COL}` ASC"
    cursor.execute(query, tuple(params))
    product_rows = cursor.fetchall()
    if not product_rows:
        print("No products found for given filters.")
        raise SystemExit

    # Indices
    idx_id = 0
    idx_item = 1
    has_media = MEDIA_COL is not None
    num_alt = len(ALT_URL_COLS)
    idx_media = 2 if has_media else -1
    idx_alt_start = (3 if has_media else 2)

    # Collect data
    all_media_ids = []
    product_media_map = {}    # (prod_id, item_id) -> [media_ids]
    product_alt_url_map = {}  # (prod_id, item_id) -> alt_path_or_url|None
    unique_items = []

    for row in product_rows:
        prod_id = str(row[idx_id])
        item_id = str(row[idx_item] if row[idx_item] is not None else row[idx_id])

        if item_id not in unique_items:
            unique_items.append(item_id)

        media_ids = []
        if has_media:
            media_raw = row[idx_media]
            if media_raw:
                try:
                    data = json.loads(media_raw) if isinstance(media_raw, (str, bytes)) else media_raw
                    if isinstance(data, list):
                        media_ids = [str(i) for i in data if i not in (None, "", "None", "none")]
                    elif isinstance(data, (int, str)):
                        s = str(data).strip()
                        if "," in s:
                            media_ids = [p.strip() for p in s.split(",") if p.strip()]
                        elif s.lower() not in ("", "none"):
                            media_ids = [s]
                except json.JSONDecodeError:
                    s = str(media_raw).strip()
                    if "," in s:
                        media_ids = [p.strip() for p in s.split(",") if p.strip()]
                    elif s.lower() not in ("", "none"):
                        media_ids = [s]

        media_ids = [mid.strip() for mid in media_ids if mid and isinstance(mid, str)]
        media_ids = dedupe_preserve_order(media_ids)
        product_media_map[(prod_id, item_id)] = media_ids
        all_media_ids.extend(media_ids)

        alt_value = None
        if num_alt:
            for i in range(num_alt):
                val = row[idx_alt_start + i]
                if val:
                    s = str(val).strip()
                    if s:
                        alt_value = s
                        break
        product_alt_url_map[(prod_id, item_id)] = alt_value

    all_media_ids = dedupe_preserve_order(all_media_ids)
    print(f"[SCAN] rows: {len(product_rows)} | unique item_ids: {len(unique_items)} | media_id refs: {len(all_media_ids)}")

    media_id_to_file = fetch_media_mapping_chunked(cursor, all_media_ids, chunk_size=900) if all_media_ids else {}

    # --- Selection: choose one image per item_id (parallel) ---
    failed_log_lines = []
    items_iter = unique_items if args.max is None else unique_items[:max(0, args.max)]
    total_to_select = len(items_iter)

    chosen_per_item = {}  # item_id -> (url, base)
    if use_tqdm:
        pbar_sel = tqdm(total=total_to_select, desc="Selecting", unit="item")
    else:
        pbar_sel = None

    def select_done_msg(item_id, url):
        if url:
            from os.path import basename
            log(f"[SELECT] {item_id} → {basename(urlparse(url).path)}")
        else:
            log(f"[MISS]   {item_id} — no alive image")

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(select_one, it, product_media_map, product_alt_url_map, media_id_to_file, failed_log_lines): it
                   for it in items_iter}
        done_cnt = 0
        for fut in as_completed(futures):
            item_id, url, base, err = fut.result()
            select_done_msg(item_id, url)
            if url:
                chosen_per_item[item_id] = (url, base)
            done_cnt += 1
            if use_tqdm:
                pbar_sel.update(1)
            elif use_simple and not use_none:
                print_simple_progress("select", done_cnt, total_to_select, ok=len(chosen_per_item), skipped=0, failed=(done_cnt - len(chosen_per_item)))

    if use_tqdm and pbar_sel is not None:
        pbar_sel.close()

    if not chosen_per_item:
        print("No images to download after selection step.")
        raise SystemExit

    # --- Download & convert (parallel) ---
    items = list(chosen_per_item.items())
    total = len(items)
    ok_cnt = 0
    skip_cnt = 0
    err_cnt = 0

    if use_tqdm:
        pbar = tqdm(total=total, desc="Downloading", unit="item")
    else:
        pbar = None

    def dl_task(item_id, pair):
        url, base = pair
        return item_id, download_one(item_id, url, base, dry_run=args.dry_run)

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(dl_task, item_id, pair): item_id for item_id, pair in items}
        done = 0
        for fut in as_completed(futures):
            item_id, (status, info) = fut.result()
            if status == "ok":
                log(f"[OK]    {item_id} → {info}")
                ok_cnt += 1
            elif status == "skip":
                log(f"[SKIP]  {item_id} → {info}")
                skip_cnt += 1
            else:
                log(f"[ERR]   {item_id} → {info}")
                err_cnt += 1

            done += 1
            if use_tqdm:
                pbar.update(1)
                pbar.set_postfix(ok=ok_cnt, skip=skip_cnt, err=err_cnt)
            elif use_simple and not use_none:
                print_simple_progress("download", done, total, ok_cnt, skip_cnt, err_cnt)

    if use_tqdm and pbar is not None:
        pbar.close()

    # Write detailed log for misses
    if LOG_FAILED and failed_log_lines and not args.dry_run:
        log_path = os.path.join(BASE_SAVE_DIR, "missing_urls.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_log_lines))
        print(f"\n[LOG] Detailed log of failed URL attempts: {log_path}")

    print(f"\n[DONE] Items targeted: {len(items)} | Saved: {ok_cnt} | Skipped: {skip_cnt} | Errors: {err_cnt}")

except mysql.connector.Error as db_err:
    print("DB Error:", db_err)
except Exception as e:
    print("Error:", e)
finally:
    try:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
    except Exception:
        pass
