from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from storage import Storage

UA = "steam-radar/1.0 (+streamlit)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

STEAM_SEARCH_URL = "https://store.steampowered.com/search/results/"
STEAM_APP_URL = "https://store.steampowered.com/app/{appid}"
APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"
GAMEDATA_WL_URL = "https://gamedata.wtf/api/wishlist/{appid}"


# ----------------------------
# Robust GET with backoff
# ----------------------------
def _get_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 6,
    base_sleep: float = 0.8,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code <= 599:
                time.sleep(min(30.0, base_sleep * (2 ** attempt)))
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(min(30.0, base_sleep * (2 ** attempt)))
    if last_exc:
        raise last_exc
    raise RuntimeError("Request failed")


# ----------------------------
# Followers (NEW)
# ----------------------------
def fetch_followers(storage: Storage, appid: int) -> Optional[int]:
    """
    Scrapes follower count from the Steam store page.
    Display-only signal. Cached aggressively.
    """
    key = f"followers_{appid}"
    cached = storage.get_json(key)
    if cached is not None:
        return cached

    try:
        url = STEAM_APP_URL.format(appid=appid)
        r = _get_with_retry(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        # Followers text usually looks like: "12,345 Followers"
        text = soup.get_text(" ", strip=True)
        m = re.search(r"([\d,]+)\s+Followers", text, re.IGNORECASE)
        if not m:
            storage.set_json(key, None, ttl_seconds=7 * 24 * 3600)
            return None

        followers = int(m.group(1).replace(",", ""))
        storage.set_json(key, followers, ttl_seconds=7 * 24 * 3600)
        return followers
    except Exception:
        storage.set_json(key, None, ttl_seconds=7 * 24 * 3600)
        return None


# ----------------------------
# Tags / search
# ----------------------------
def fetch_global_tags(storage: Storage, cc: str) -> List[Dict[str, Any]]:
    key = f"global_tags_{cc}"
    cached = storage.get_json(key)
    if cached:
        return cached

    params = {
        "query": "",
        "start": 0,
        "count": 50,
        "cc": cc,
        "l": "english",
        "infinite": 1,
    }
    r = _get_with_retry(STEAM_SEARCH_URL, params=params)
    data = r.json()
    facets = data.get("facets", {})
    tags = facets.get("tags", [])
    tags_sorted = sorted(tags, key=lambda x: x.get("count", 0), reverse=True)

    storage.set_json(key, tags_sorted, ttl_seconds=7 * 24 * 3600)
    return tags_sorted


def fetch_appids(
    storage: Storage,
    country: str,
    pages: int,
    per_page: int,
    sort_by: str,
    include_tagids: Optional[List[int]] = None,
) -> List[int]:
    include_tagids = include_tagids or []
    appids: List[int] = []

    for page in range(pages):
        start = page * per_page
        params = {
            "query": "",
            "start": start,
            "count": per_page,
            "cc": country,
            "l": "english",
            "infinite": 1,
            "sort_by": sort_by,
        }
        if include_tagids:
            params["tags"] = ",".join(str(t) for t in include_tagids)

        r = _get_with_retry(STEAM_SEARCH_URL, params=params)
        html = r.json().get("results_html", "")
        appids.extend(_extract_appids_from_search_html(html))
        time.sleep(0.15)

    return _dedupe(appids)


def fetch_upcoming_appids(
    storage: Storage,
    country: str,
    pages: int,
    per_page: int,
) -> List[int]:
    appids: List[int] = []
    for page in range(pages):
        start = page * per_page
        params = {
            "query": "",
            "start": start,
            "count": per_page,
            "cc": country,
            "l": "english",
            "infinite": 1,
            "filter": "comingsoon",
            "sort_by": "Released_ASC",
        }
        r = _get_with_retry(STEAM_SEARCH_URL, params=params)
        html = r.json().get("results_html", "")
        appids.extend(_extract_appids_from_search_html(html))
        time.sleep(0.15)

    return _dedupe(appids)


def _extract_appids_from_search_html(html: str) -> List[int]:
    soup = BeautifulSoup(html, "html.parser")
    ids: List[int] = []
    for a in soup.select("a.search_result_row"):
        appid = a.get("data-ds-appid") or a.get("data-ds-bundleid")
        if not appid:
            continue
        try:
            ids.append(int(appid.split(",")[0]))
        except Exception:
            continue
    return ids


def _dedupe(items: List[int]) -> List[int]:
    seen, out = set(), []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


# ----------------------------
# Appdetails / reviews
# ----------------------------
def fetch_appdetails_batch(
    storage: Storage,
    appids: List[int],
    cc: str,
    batch_size: int = 25,
    per_request_sleep: float = 0.35,
) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(appids), batch_size):
        for appid in appids[i:i + batch_size]:
            try:
                data = fetch_appdetails(storage, appid, cc)
                if data:
                    out[appid] = data
            except Exception:
                pass
            time.sleep(per_request_sleep)
    return out


def fetch_appdetails(storage: Storage, appid: int, cc: str) -> Optional[Dict[str, Any]]:
    key = f"appdetails_{appid}_{cc}"
    cached = storage.get_json(key)
    if cached:
        return cached

    r = _get_with_retry(APPDETAILS_URL, {"appids": appid, "cc": cc, "l": "english"})
    payload = r.json().get(str(appid))
    if not payload or not payload.get("success"):
        return None

    data = payload.get("data")
    if isinstance(data, dict):
        storage.set_json(key, data, ttl_seconds=14 * 24 * 3600)
        return data
    return None


def fetch_reviews(storage: Storage, appid: int, cc: str, days: int) -> Dict[str, int]:
    key = f"reviews_{appid}_{cc}_{days}"
    cached = storage.get_json(key)
    if cached:
        return cached

    r = _get_with_retry(
        REVIEWS_URL.format(appid=appid),
        {
            "json": 1,
            "language": "all",
            "purchase_type": "all",
            "day_range": days,
            "filter": "recent",
            "cc": cc,
        },
    )
    summary = r.json().get("query_summary", {})
    out = {
        "reviews": int(summary.get("total_reviews", 0) or 0),
        "positive": int(summary.get("total_positive", 0) or 0),
    }
    storage.set_json(key, out, ttl_seconds=6 * 3600)
    return out


def fetch_wishlist_estimate_gamedata(storage: Storage, appid: int) -> Optional[int]:
    key = f"wl_gamedata_{appid}"
    cached = storage.get_json(key)
    if cached is not None:
        return cached

    try:
        r = SESSION.get(GAMEDATA_WL_URL.format(appid=appid), timeout=20)
        if r.status_code != 200:
            storage.set_json(key, None, ttl_seconds=6 * 3600)
            return None
        payload = r.json()
        for k in ("wishlists", "wishlist_estimate", "estimate"):
            if k in payload and payload[k] is not None:
                val = int(payload[k])
                storage.set_json(key, val, ttl_seconds=6 * 3600)
                return val
        storage.set_json(key, None, ttl_seconds=6 * 3600)
        return None
    except Exception:
        storage.set_json(key, None, ttl_seconds=6 * 3600)
        return None


def parse_release(appdata: Dict[str, Any]) -> Optional[datetime]:
    ds = (appdata.get("release_date") or {}).get("date")
    if not ds:
        return None
    try:
        dt = dateparser.parse(ds, fuzzy=True)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def release_date_text(appdata: Dict[str, Any]) -> str:
    return str((appdata.get("release_date") or {}).get("date") or "").strip()


def is_coming_soon(appdata: Dict[str, Any]) -> bool:
    return bool((appdata.get("release_date") or {}).get("coming_soon"))


def extract_genre_category_terms(appdata: Dict[str, Any]) -> List[str]:
    terms: List[str] = []
    for g in appdata.get("genres") or []:
        if g.get("description"):
            terms.append(g["description"])
    for c in appdata.get("categories") or []:
        if c.get("description"):
            terms.append(c["description"])
    seen, out = set(), []
    for t in terms:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out
