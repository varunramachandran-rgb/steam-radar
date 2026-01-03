from __future__ import annotations

import json
import math
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# -----------------------------
# URLs
# -----------------------------
STEAM_SEARCH_URL = "https://store.steampowered.com/search/results/"
APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"
GLOBAL_TAGS_URL = "https://store.steampowered.com/tag/browse/?cc={cc}"

# gamedata.wtf estimate endpoint is not documented; we keep it best-effort
GAMEDATA_WL_URL = "https://gamedata.wtf/app/{appid}.json"

# Followers: read from store page HTML
STORE_PAGE_URL = "https://store.steampowered.com/app/{appid}/?cc={cc}&l=english"


# -----------------------------
# Caching helpers (Storage adapter)
# -----------------------------
def _cache_get(storage, key: str) -> Optional[Dict[str, Any]]:
    """
    Supports the Storage class you already have (cache_get returning CacheEntry with `.value` dict).
    """
    try:
        entry = storage.cache_get(key)
        if not entry:
            return None
        # Your storage.py uses CacheEntry(value: Dict[str, Any], fetched_at, ttl_seconds)
        return entry.value
    except Exception:
        return None


def _cache_set(storage, key: str, value: Dict[str, Any], ttl_seconds: int) -> None:
    try:
        storage.cache_set(key, value, ttl_seconds)
    except Exception:
        pass


# -----------------------------
# HTTP helpers (rate limit safe)
# -----------------------------
_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; SteamRadar/1.0; +https://store.steampowered.com/)",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def _get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20, max_retries: int = 6) -> requests.Response:
    """
    GET with exponential backoff for 429/5xx.
    """
    for attempt in range(max_retries):
        r = _SESSION.get(url, params=params, timeout=timeout)

        if r.status_code in (429, 500, 502, 503, 504):
            # exponential backoff + jitter
            sleep_s = min(8.0, (0.75 * (2 ** attempt))) + random.random() * 0.25
            time.sleep(sleep_s)
            continue

        return r

    # last try (let raise_for_status surface details)
    return r


# -----------------------------
# Global tags (Steam browse page)
# -----------------------------
def fetch_global_tags(storage, cc: str) -> List[Dict[str, Any]]:
    """
    Returns list of {name: str, count: int} in decreasing order.
    Best-effort: parses Steam 'Global Tags' browse page.
    """
    cc = (cc or "US").upper()
    key = f"global_tags::{cc}"
    cached = _cache_get(storage, key)
    if cached:
        return cached.get("tags", [])

    r = _get(GLOBAL_TAGS_URL.format(cc=cc))
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    tags: List[Dict[str, Any]] = []
    # The left list items are <a class="tag_browse_tag">Tag</a> but Steam may vary.
    for a in soup.select("a.tag_browse_tag"):
        name = a.get_text(strip=True)
        if not name:
            continue
        tags.append({"name": name, "count": None})

    # Steam already sorts by frequency on that page.
    _cache_set(storage, key, {"tags": tags}, ttl_seconds=60 * 60 * 24)
    return tags


# -----------------------------
# Steam search: fetch appids
# -----------------------------
def _parse_search_appids(html: str) -> List[int]:
    soup = BeautifulSoup(html, "html.parser")
    appids: List[int] = []
    for a in soup.select("a.search_result_row"):
        appid = a.get("data-ds-appid") or a.get("data-ds-packageid")
        if not appid:
            continue
        # data-ds-appid can be "123,456" sometimes
        first = str(appid).split(",")[0].strip()
        if first.isdigit():
            appids.append(int(first))
    return appids


def fetch_appids(
    storage,
    country: str,
    pages: int,
    per_page: int,
    sort_by: str = "Released_DESC",
    include_tagids: Optional[List[int]] = None,
) -> List[int]:
    """
    New releases: uses Steam search endpoint.
    """
    country = (country or "US").upper()
    include_tagids = include_tagids or []

    appids: List[int] = []
    for p in range(pages):
        start = p * per_page
        params = {
            "cc": country,
            "l": "english",
            "start": start,
            "count": per_page,
            "sort_by": sort_by,
            "infinite": 1,
            "category1": 998,  # Games
        }
        # Tags can be applied via "tags" param in some search flows, but we keep tags local for speed.
        r = _get(STEAM_SEARCH_URL, params=params)
        r.raise_for_status()

        # Search results endpoint returns JSON with "results_html"
        payload = r.json()
        html = payload.get("results_html", "")
        page_appids = _parse_search_appids(html)
        appids.extend(page_appids)

    # Unique preserve order
    seen = set()
    out: List[int] = []
    for a in appids:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def fetch_upcoming_appids(
    storage,
    country: str,
    pages: int,
    per_page: int,
) -> List[int]:
    """
    Upcoming: uses Steam search but with 'Coming Soon' filters.
    We intentionally do NOT require strict flags here, because Steam can change them.
    """
    country = (country or "US").upper()

    appids: List[int] = []
    for p in range(pages):
        start = p * per_page
        params = {
            "cc": country,
            "l": "english",
            "start": start,
            "count": per_page,
            "infinite": 1,
            "category1": 998,  # Games
            "os": "win",
            "filter": "comingsoon",  # key difference vs new releases
        }
        r = _get(STEAM_SEARCH_URL, params=params)
        r.raise_for_status()
        payload = r.json()
        html = payload.get("results_html", "")
        page_appids = _parse_search_appids(html)
        appids.extend(page_appids)

    seen = set()
    out: List[int] = []
    for a in appids:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


# -----------------------------
# appdetails (batched)
# -----------------------------
def fetch_appdetails(storage, appid: int, cc: str) -> Optional[Dict[str, Any]]:
    cc = (cc or "US").upper()
    key = f"appdetails::{appid}::{cc}"
    cached = _cache_get(storage, key)
    if cached:
        return cached.get("data")

    params = {"appids": int(appid), "cc": cc, "l": "english"}
    r = _get(APPDETAILS_URL, params=params)
    r.raise_for_status()
    payload = r.json()

    block = payload.get(str(appid), {})
    if not block or not block.get("success"):
        _cache_set(storage, key, {"data": None}, ttl_seconds=60 * 30)
        return None

    data = block.get("data")
    _cache_set(storage, key, {"data": data}, ttl_seconds=60 * 60 * 24)
    return data


def fetch_appdetails_batch(
    storage,
    appids: List[int],
    cc: str,
    batch_size: int = 25,
    per_request_sleep: float = 0.45,
) -> Dict[int, Optional[Dict[str, Any]]]:
    """
    Steam appdetails endpoint is 1 appid per call. This helper batches sequentially
    and respects a configurable delay to avoid 429.
    """
    out: Dict[int, Optional[Dict[str, Any]]] = {}
    cc = (cc or "US").upper()

    for i, appid in enumerate(appids):
        out[int(appid)] = fetch_appdetails(storage, int(appid), cc)
        if per_request_sleep > 0:
            time.sleep(per_request_sleep)
    return out


# -----------------------------
# Reviews
# -----------------------------
def fetch_reviews(storage, appid: int, cc: str, lookback_days: int) -> Dict[str, int]:
    """
    Uses Steam reviews API. We keep it lightweight: total reviews and positive count.
    """
    cc = (cc or "US").upper()
    lookback_days = max(1, int(lookback_days))

    key = f"reviews::{appid}::{cc}::{lookback_days}"
    cached = _cache_get(storage, key)
    if cached:
        return {"reviews": int(cached["reviews"]), "positive": int(cached["positive"])}

    params = {
        "json": 1,
        "filter": "recent",
        "language": "all",
        "purchase_type": "all",
        "day_range": lookback_days,
    }
    r = _get(REVIEWS_URL.format(appid=int(appid)), params=params)
    r.raise_for_status()
    payload = r.json()

    q = payload.get("query_summary", {}) or {}
    total = int(q.get("total_reviews", 0) or 0)
    pos = int(q.get("total_positive", 0) or 0)

    _cache_set(storage, key, {"reviews": total, "positive": pos}, ttl_seconds=60 * 30)
    return {"reviews": total, "positive": pos}


# -----------------------------
# Release parsing (robust)
# -----------------------------
_UPCOMING_HINT_RE = re.compile(
    r"(coming soon|tba|to be announced|q[1-4]\s*\d{4}|\d{4}|early access)",
    re.IGNORECASE,
)


def release_date_text(data: Dict[str, Any]) -> str:
    rd = (data or {}).get("release_date") or {}
    txt = (rd.get("date") or "").strip()
    if txt:
        return txt
    return ""


def parse_release(data: Dict[str, Any]) -> Optional[datetime]:
    """
    Best-effort parsing of appdetails release_date.
    Returns aware datetime in UTC (midnight).
    """
    rd = (data or {}).get("release_date") or {}
    date_str = (rd.get("date") or "").strip()
    if not date_str:
        return None

    # Steam can return formats like "27 Dec, 2025" or "Dec 27, 2025" etc.
    patterns = [
        "%d %b, %Y",
        "%b %d, %Y",
        "%d %B, %Y",
        "%B %d, %Y",
        "%Y-%m-%d",
    ]
    for fmt in patterns:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # If Steam gives only a year or quarter, treat as unknown (None)
    if re.fullmatch(r"\d{4}", date_str):
        return None
    if re.match(r"q[1-4]\s*\d{4}", date_str, re.IGNORECASE):
        return None

    return None


def is_coming_soon(data: Dict[str, Any]) -> bool:
    """
    Robust coming-soon detection:
    - uses release_date.coming_soon if present
    - OR scans release_date_text for common future/unknown patterns
    """
    rd = (data or {}).get("release_date") or {}
    if isinstance(rd.get("coming_soon"), bool) and rd.get("coming_soon") is True:
        return True

    txt = release_date_text(data)
    if not txt:
        return False

    return bool(_UPCOMING_HINT_RE.search(txt))


def classify_upcoming(
    data: Dict[str, Any],
    now_utc: datetime,
    window_days: int,
    include_unknown: bool,
) -> Tuple[bool, Optional[int]]:
    """
    Returns (keep, days_until).

    Keep logic:
    - If release_dt parses and is in future: keep only if within window_days.
    - Else if release looks like "coming soon / tba / qX YYYY / YYYY": keep if include_unknown.
    - Else: discard.
    """
    release_dt = parse_release(data)
    if release_dt:
        delta_days = (release_dt - now_utc).total_seconds() / 86400.0
        if delta_days < 0:
            return (False, None)
        if delta_days > window_days:
            return (False, None)
        return (True, int(round(delta_days)))

    # Unknown/approx date strings
    if is_coming_soon(data):
        return (include_unknown, None)

    # If release_date_text exists but doesn't match: treat as unknown; allow only if include_unknown
    txt = release_date_text(data)
    if txt:
        return (include_unknown, None)

    return (False, None)


# -----------------------------
# Genres / categories extraction
# -----------------------------
def extract_genre_category_terms(data: Dict[str, Any]) -> List[str]:
    terms: List[str] = []

    for g in (data.get("genres") or []):
        d = (g or {}).get("description")
        if d:
            terms.append(str(d))

    for c in (data.get("categories") or []):
        d = (c or {}).get("description")
        if d:
            terms.append(str(d))

    # de-dupe preserve order
    seen = set()
    out: List[str] = []
    for t in terms:
        t2 = t.strip()
        if not t2:
            continue
        if t2.lower() not in seen:
            seen.add(t2.lower())
            out.append(t2)
    return out


# -----------------------------
# Wishlists estimate (gamedata.wtf) - best effort
# -----------------------------
def fetch_wishlist_estimate_gamedata(storage, appid: int) -> Optional[int]:
    key = f"wl_est::{appid}"
    cached = _cache_get(storage, key)
    if cached and "wishlists" in cached:
        return cached.get("wishlists")

    try:
        r = _get(GAMEDATA_WL_URL.format(appid=int(appid)), timeout=20, max_retries=4)
        if r.status_code != 200:
            _cache_set(storage, key, {"wishlists": None}, ttl_seconds=60 * 60)
            return None
        payload = r.json()
        wl = payload.get("wishlists") or payload.get("wishlist") or payload.get("wl")
        if wl is None:
            _cache_set(storage, key, {"wishlists": None}, ttl_seconds=60 * 60)
            return None
        wl_int = int(wl)
        _cache_set(storage, key, {"wishlists": wl_int}, ttl_seconds=60 * 60 * 6)
        return wl_int
    except Exception:
        _cache_set(storage, key, {"wishlists": None}, ttl_seconds=60 * 60)
        return None


# -----------------------------
# Followers (Steam store page scrape) - best effort
# -----------------------------
_FOLLOWERS_RE = re.compile(r"([\d,]+)\s+followers", re.IGNORECASE)


def fetch_followers(storage, appid: int) -> Optional[int]:
    key = f"followers::{appid}"
    cached = _cache_get(storage, key)
    if cached and "followers" in cached:
        return cached.get("followers")

    try:
        r = _get(STORE_PAGE_URL.format(appid=int(appid), cc="US"), timeout=20, max_retries=4)
        if r.status_code != 200:
            _cache_set(storage, key, {"followers": None}, ttl_seconds=60 * 60)
            return None
        m = _FOLLOWERS_RE.search(r.text)
        if not m:
            _cache_set(storage, key, {"followers": None}, ttl_seconds=60 * 60)
            return None
        val = int(m.group(1).replace(",", ""))
        _cache_set(storage, key, {"followers": val}, ttl_seconds=60 * 60 * 12)
        return val
    except Exception:
        _cache_set(storage, key, {"followers": None}, ttl_seconds=60 * 60)
        return None
