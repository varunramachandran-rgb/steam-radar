from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from io import BytesIO
import traceback

import pandas as pd
import streamlit as st
import pycountry

from storage import Storage
from steam_sources import (
    fetch_global_tags,
    fetch_appids,
    fetch_upcoming_appids,
    fetch_appdetails_batch,
    fetch_reviews,
    fetch_wishlist_estimate_gamedata,
    fetch_followers,
    parse_release,
    release_date_text,
    extract_genre_category_terms,
)

# -----------------------------------------------------------------------------
# Page config + header
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Steam Radar", layout="wide")
storage = Storage()

st.markdown(
    """
    <h1 style="display:flex; align-items:center; gap:10px; margin-bottom:0.25rem;">
        <img src="https://store.cloudflare.steamstatic.com/public/shared/images/header/globalheader_logo.png"
             width="32"/>
        Steam Radar 📡
    </h1>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _build_all_country_options() -> Tuple[List[str], Dict[str, str]]:
    items = [(c.alpha_2, c.name) for c in pycountry.countries]

    # Useful Steam-adjacent extras
    extras = [
        ("XK", "Kosovo"),
        ("HK", "Hong Kong"),
        ("MO", "Macao"),
        ("TW", "Taiwan"),
    ]
    existing = {cc for cc, _ in items}
    for cc, name in extras:
        if cc not in existing:
            items.append((cc, name))

    items.sort(key=lambda x: x[1])
    labels = [f"{name} ({cc})" for cc, name in items]
    label_to_cc = {f"{name} ({cc})": cc for cc, name in items}
    return labels, label_to_cc


ALL_COUNTRY_OPTIONS, COUNTRY_LABEL_TO_CC = _build_all_country_options()
CC_TO_COUNTRY_LABEL = {v: k for k, v in COUNTRY_LABEL_TO_CC.items()}
ALL_CC = sorted(list(CC_TO_COUNTRY_LABEL.keys()))


def _filter_existing(ccs: List[str]) -> List[str]:
    return [cc for cc in ccs if cc in CC_TO_COUNTRY_LABEL]


def _selected_ccs_from_labels(labels: List[str]) -> List[str]:
    return [COUNTRY_LABEL_TO_CC[x] for x in labels]


def classify_upcoming(
    data: Dict[str, Any],
    now_utc: datetime,
    window_days: int,
    include_unknown: bool,
) -> Tuple[bool, Optional[float]]:
    """
    Upcoming inclusion rules:
    - If a concrete release datetime exists: keep if 0 <= days_until <= window_days
    - If no concrete date: keep only if include_unknown is True
    Returns (keep, days_until)
    """
    rel = parse_release(data)
    if rel is None:
        return (include_unknown, None)

    days_until = (rel - now_utc).total_seconds() / 86400.0
    if 0 <= days_until <= float(window_days):
        return (True, days_until)
    return (False, days_until)


# -----------------------------------------------------------------------------
# Country tiers (edit anytime)
# -----------------------------------------------------------------------------
PRIMARY_CC = _filter_existing(["US", "GB", "DE", "FR", "CA", "JP", "KR", "TW"])
SECONDARY_CC = _filter_existing(
    [
        "IN", "BR", "MX", "AR", "CL", "CO",
        "TR", "AE", "SA", "EG",
        "PL", "CZ", "HU", "RO",
        "ID", "TH", "VN", "PH", "MY", "SG",
        "AU", "NZ",
    ]
)

TIER_TO_CC = {
    "Primary (early conversion signal)": PRIMARY_CC,
    "Secondary (emerging / price-sensitive signal)": SECONDARY_CC,
    "All countries": ALL_CC,
    "Custom": [],
}

# -----------------------------------------------------------------------------
# Session state defaults
# -----------------------------------------------------------------------------
if "country_tier" not in st.session_state:
    st.session_state["country_tier"] = "Primary (early conversion signal)"
if "country_labels_selected" not in st.session_state:
    st.session_state["country_labels_selected"] = [CC_TO_COUNTRY_LABEL[c] for c in PRIMARY_CC]

if "perf_preset" not in st.session_state:
    st.session_state["perf_preset"] = "Normal"
if "batch_size" not in st.session_state:
    st.session_state["batch_size"] = 25
if "per_request_sleep" not in st.session_state:
    st.session_state["per_request_sleep"] = 0.45

if "include_tags" not in st.session_state:
    st.session_state["include_tags"] = []
if "all_tags_no_filter" not in st.session_state:
    st.session_state["all_tags_no_filter"] = True

# Text include/exclude chip lists
if "include_terms_list" not in st.session_state:
    st.session_state["include_terms_list"] = []
if "exclude_terms_list" not in st.session_state:
    st.session_state["exclude_terms_list"] = []

# Export dropdown remembered
if "export_format" not in st.session_state:
    st.session_state["export_format"] = "Export as .csv"

# Persisted scan outputs so reruns (export dropdown etc.) don't clear results
if "last_display_df" not in st.session_state:
    st.session_state["last_display_df"] = None
if "last_mode_label" not in st.session_state:
    st.session_state["last_mode_label"] = None
if "last_run_date" not in st.session_state:
    st.session_state["last_run_date"] = None
if "last_dbg" not in st.session_state:
    st.session_state["last_dbg"] = None
if "last_exceptions" not in st.session_state:
    st.session_state["last_exceptions"] = []


def _apply_country_tier():
    tier = st.session_state["country_tier"]
    if tier == "Custom":
        return
    ccs = TIER_TO_CC.get(tier, [])
    st.session_state["country_labels_selected"] = [CC_TO_COUNTRY_LABEL[cc] for cc in ccs if cc in CC_TO_COUNTRY_LABEL]


def _infer_custom_tier():
    if st.session_state["country_tier"] == "Custom":
        return
    current_ccs = sorted(_selected_ccs_from_labels(st.session_state["country_labels_selected"]))
    tier_ccs = sorted(TIER_TO_CC.get(st.session_state["country_tier"], []))
    if current_ccs != tier_ccs:
        st.session_state["country_tier"] = "Custom"


def _apply_perf_preset():
    p = st.session_state["perf_preset"]
    if p == "Safe":
        st.session_state["batch_size"] = 20
        st.session_state["per_request_sleep"] = 0.70
    elif p == "Normal":
        st.session_state["batch_size"] = 25
        st.session_state["per_request_sleep"] = 0.45
    elif p == "Fast":
        st.session_state["batch_size"] = 35
        st.session_state["per_request_sleep"] = 0.30


def _add_term(key_in: str, key_list: str):
    raw = st.session_state.get(key_in, "")
    term = raw.strip().lower()
    if not term:
        return
    if term not in st.session_state[key_list]:
        st.session_state[key_list].append(term)
    st.session_state[key_in] = ""


def _remove_term(term: str, key_list: str):
    st.session_state[key_list] = [t for t in st.session_state[key_list] if t != term]


# -----------------------------------------------------------------------------
# Sidebar UI
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("Settings")
    mode = st.radio("Mode", ["New releases", "Upcoming"], index=0)

    # --- Countries & tiers ---
    st.subheader("Countries")
    st.selectbox(
        "Country tier",
        list(TIER_TO_CC.keys()),
        key="country_tier",
        on_change=_apply_country_tier,
        help="Selecting a tier automatically selects all countries inside it.",
    )

    if st.session_state["country_tier"] == "All countries":
        all_labels = [CC_TO_COUNTRY_LABEL[cc] for cc in ALL_CC]
        st.session_state["country_labels_selected"] = all_labels
        preview = ", ".join(ALL_CC[:12]) + ("…" if len(ALL_CC) > 12 else "")
        st.info(f"Selected: {len(ALL_CC)} countries. Codes: {preview}")
        selected_labels = st.session_state["country_labels_selected"]
    else:
        selected_labels = st.multiselect(
            "Countries (searchable)",
            options=ALL_COUNTRY_OPTIONS,
            key="country_labels_selected",
        )
        _infer_custom_tier()

    countries = _selected_ccs_from_labels(selected_labels)

    # --- Mode-specific basic filters ---
    if mode == "New releases":
        window_days = st.slider("Released in last N days", 3, 60, 14)
        sort_by = "Released_DESC"
    else:
        window_days = st.slider("Releasing in next N days", 7, 180, 60)
        sort_by = "Released_ASC"

    pages = st.slider("Search depth (pages)", 1, 20, 3)
    per_page = st.selectbox("Results per page", [50, 100], index=0)

    # --- Performance controls ---
    st.subheader("Performance controls")
    max_apps = st.slider("Max apps to process per scan (total)", 50, 2000, 500, step=50)
    show_top_n = st.slider("Show top N results", 10, 500, 60, step=10)

    st.selectbox(
        "Scan speed preset",
        ["Safe", "Normal", "Fast"],
        key="perf_preset",
        on_change=_apply_perf_preset,
    )
    batch_size = st.slider("Batch size for appdetails", 10, 50, step=5, key="batch_size")
    per_request_sleep = st.slider("Delay between appdetails requests (sec)", 0.10, 1.50, step=0.05, key="per_request_sleep")

    # --- Tags ---
    st.subheader("Steam Tag filters (OR match)")
    cc_for_tags = "US"

    # If tag fetch fails, we keep the UI usable and log exception
    try:
        global_tags = fetch_global_tags(storage, cc_for_tags)
        tag_names_ordered = [t["name"] for t in global_tags]
    except Exception:
        st.session_state["last_exceptions"].append(
            {
                "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "stage": "fetch_global_tags",
                "country": cc_for_tags,
                "appid": None,
                "error": "Failed to load global tags",
                "trace": traceback.format_exc(),
            }
        )
        tag_names_ordered = []

    col_a, col_b = st.columns([1, 1])
    with col_a:
        st.checkbox("All tags (no filtering)", key="all_tags_no_filter")
    with col_b:
        if st.button("Clear tags"):
            st.session_state["include_tags"] = []
            st.session_state["all_tags_no_filter"] = True
            st.rerun()

    include_tags = st.multiselect(
        "Include Steam Tags (OR)",
        options=tag_names_ordered,
        default=st.session_state["include_tags"],
        key="include_tags",
        disabled=st.session_state["all_tags_no_filter"],
    )

    # --- Optional enrichments ---
    st.subheader("Wishlists (optional)")
    show_wishlists = st.checkbox("Show wishlist estimates (gamedata.wtf)", value=True)

    st.subheader("Followers (optional)")
    show_followers = st.checkbox("Show followers (Steam page scrape)", value=True)

    # --- Text include/exclude chips ---
    st.subheader("Text include/exclude (optional)")
    st.caption("Matches against: name, developer, publisher, plus genres/categories from appdetails.")

    c1, c2 = st.columns([3, 1])
    with c1:
        st.text_input("Include term (OR match)", key="include_term_input", placeholder="Type a term and click Add")
    with c2:
        st.button("Add", key="add_include_btn", on_click=_add_term, args=("include_term_input", "include_terms_list"))

    if st.session_state["include_terms_list"]:
        cols = st.columns(4)
        for i, term in enumerate(st.session_state["include_terms_list"]):
            with cols[i % 4]:
                st.button(f"✕ {term}", key=f"rm_inc_{term}_{i}", on_click=_remove_term, args=(term, "include_terms_list"))
    else:
        st.caption("No include terms yet.")

    c3, c4 = st.columns([3, 1])
    with c3:
        st.text_input("Exclude term (hard filter)", key="exclude_term_input", placeholder="Type a term and click Add")
    with c4:
        st.button("Add", key="add_exclude_btn", on_click=_add_term, args=("exclude_term_input", "exclude_terms_list"))

    if st.session_state["exclude_terms_list"]:
        cols = st.columns(4)
        for i, term in enumerate(st.session_state["exclude_terms_list"]):
            with cols[i % 4]:
                st.button(f"✕ {term}", key=f"rm_exc_{term}_{i}", on_click=_remove_term, args=(term, "exclude_terms_list"))
    else:
        st.caption("No exclude terms yet.")

    include_terms = st.session_state["include_terms_list"]
    exclude_terms = st.session_state["exclude_terms_list"]

    # --- Mode-specific review controls ---
    if mode == "New releases":
        review_days = st.selectbox("Review lookback (days)", [1, 3, 7, 14], index=1)
        min_reviews_per_day = st.slider("Minimum reviews/day", 0.0, 20.0, 0.5, 0.1)
    else:
        include_unknown_upcoming = st.checkbox(
            "Include unknown release dates (Coming Soon / Q1 2026 / TBA)",
            value=True,
        )

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
run = st.button("Run Scan", type="primary")

if run:
    # Reset exceptions for THIS run (keep history if you prefer; tell me)
    st.session_state["last_exceptions"] = []

    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []

    active_countries = countries[:] if countries else []
    if not active_countries:
        st.warning("Select at least one country.")
        st.stop()

    selected_tags_lower = set()
    if not st.session_state["all_tags_no_filter"]:
        selected_tags_lower = {t.strip().lower() for t in include_tags if t.strip()}

    processed_apps = 0
    progress = st.progress(0)
    status = st.empty()

    dbg = {
        "non_game_type": 0,
        "missing_details": 0,
        "filtered_tag_or": 0,
        "filtered_include_terms": 0,
        "filtered_exclude_terms": 0,
        "upcoming_classify_reject": 0,
        "newrelease_wrong_window": 0,
        "newrelease_no_date": 0,
        "kept": 0,
        "exceptions": 0,
    }

    per_country_budget = max(1, max_apps // len(active_countries))

    def _update_ui(cc: str):
        pct = min(1.0, processed_apps / max(1, max_apps))
        progress.progress(pct)
        status.write(f"Scanning… {cc} | Processed apps: {processed_apps}/{max_apps}")

    for cc in active_countries:
        if processed_apps >= max_apps:
            break

        _update_ui(cc)

        try:
            if mode == "Upcoming":
                appids = fetch_upcoming_appids(storage=storage, country=cc, pages=pages, per_page=per_page)
            else:
                appids = fetch_appids(
                    storage=storage,
                    country=cc,
                    pages=pages,
                    per_page=per_page,
                    sort_by=sort_by,
                    include_tagids=[],
                )
        except Exception:
            dbg["exceptions"] += 1
            st.session_state["last_exceptions"].append(
                {
                    "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "stage": "fetch_appids" if mode != "Upcoming" else "fetch_upcoming_appids",
                    "country": cc,
                    "appid": None,
                    "error": "Failed fetching app list",
                    "trace": traceback.format_exc(),
                }
            )
            continue

        remaining_global = max(0, max_apps - processed_apps)
        budget = min(per_country_budget, remaining_global)
        appids = appids[:budget]

        try:
            details_map = fetch_appdetails_batch(
                storage, appids, cc,
                batch_size=batch_size,
                per_request_sleep=per_request_sleep,
            )
        except Exception:
            dbg["exceptions"] += 1
            st.session_state["last_exceptions"].append(
                {
                    "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "stage": "fetch_appdetails_batch",
                    "country": cc,
                    "appid": None,
                    "error": "Failed fetching appdetails batch",
                    "trace": traceback.format_exc(),
                }
            )
            continue

        for appid in appids:
            processed_apps += 1
            if processed_apps % 25 == 0:
                _update_ui(cc)

            try:
                data = details_map.get(appid)
                if data is None:
                    dbg["missing_details"] += 1
                    continue

                if data.get("type") != "game":
                    dbg["non_game_type"] += 1
                    continue

                name = data.get("name") or ""
                developer = ", ".join(data.get("developers", []) or [])
                publisher = ", ".join(data.get("publishers", []) or [])
                genre_terms = extract_genre_category_terms(data)
                genres_joined = ", ".join(genre_terms)

                if selected_tags_lower:
                    game_terms_lower = {t.strip().lower() for t in genre_terms if t.strip()}
                    if game_terms_lower.isdisjoint(selected_tags_lower):
                        dbg["filtered_tag_or"] += 1
                        continue

                blob = " ".join([name, developer, publisher] + genre_terms).lower()

                if include_terms and not any(t in blob for t in include_terms):
                    dbg["filtered_include_terms"] += 1
                    continue
                if exclude_terms and any(t in blob for t in exclude_terms):
                    dbg["filtered_exclude_terms"] += 1
                    continue

                if mode == "New releases":
                    release_dt = parse_release(data)
                    if release_dt is None:
                        dbg["newrelease_no_date"] += 1
                        continue

                    age_days = (now - release_dt).total_seconds() / 86400.0
                    if age_days < 0 or age_days > window_days:
                        dbg["newrelease_wrong_window"] += 1
                        continue

                    reviews = fetch_reviews(storage, appid, cc, review_days)
                    velocity = reviews["reviews"] / max(1, review_days)
                    positivity = (reviews["positive"] / reviews["reviews"] * 100) if reviews["reviews"] > 0 else None

                    rows.append(
                        {
                            "AppID": appid,
                            "Name": name,
                            "Store": f"https://store.steampowered.com/app/{appid}/",
                            "Developer": developer,
                            "Publisher": publisher,
                            "Country": cc,
                            "Release Date": release_dt.date(),
                            "Genres/Categories": genres_joined,
                            "Reviews/day": round(velocity, 2),
                            "% Positive": None if positivity is None else round(positivity, 1),
                            "_reviews_total": int(reviews["reviews"]),
                            "_reviews_pos": int(reviews["positive"]),
                        }
                    )
                    dbg["kept"] += 1
                else:
                    keep, days_until = classify_upcoming(
                        data=data,
                        now_utc=now,
                        window_days=window_days,
                        include_unknown=include_unknown_upcoming,
                    )
                    if not keep:
                        dbg["upcoming_classify_reject"] += 1
                        continue

                    rows.append(
                        {
                            "AppID": appid,
                            "Name": name,
                            "Store": f"https://store.steampowered.com/app/{appid}/",
                            "Developer": developer,
                            "Publisher": publisher,
                            "Country": cc,
                            "Release": release_date_text(data) or "Coming Soon",
                            "Days Until": days_until,
                            "Genres/Categories": genres_joined,
                        }
                    )
                    dbg["kept"] += 1

            except Exception:
                dbg["exceptions"] += 1
                st.session_state["last_exceptions"].append(
                    {
                        "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "stage": "process_app",
                        "country": cc,
                        "appid": int(appid) if appid is not None else None,
                        "error": "Unhandled error while processing app",
                        "trace": traceback.format_exc(),
                    }
                )
                continue

        _update_ui(cc)

    progress.progress(1.0)
    status.write(f"Done. Processed apps: {processed_apps}/{max_apps}")

    if not rows:
        st.warning("No results found. Increase pages/max apps, change countries, or relax filters.")
        # Still persist debug + exceptions
        st.session_state["last_dbg"] = dbg
        st.stop()

    df = pd.DataFrame(rows)
    countries_agg = df.groupby("AppID")["Country"].apply(lambda s: ", ".join(sorted(set(s)))).rename("Countries")

    if mode == "New releases":
        first_fields = df.groupby("AppID").agg(
            {
                "Name": "first",
                "Store": "first",
                "Developer": "first",
                "Publisher": "first",
                "Release Date": "first",
                "Genres/Categories": "first",
            }
        )
        reviews_day_max = df.groupby("AppID")["Reviews/day"].max().rename("Reviews/day")

        totals = df.groupby("AppID")[["_reviews_total", "_reviews_pos"]].sum()
        pct_pos = (totals["_reviews_pos"] / totals["_reviews_total"] * 100).round(1).rename("% Positive")

        out = pd.concat([first_fields, countries_agg, reviews_day_max, pct_pos], axis=1).reset_index()

        out["Wishlists (est.)"] = None
        if show_wishlists:
            try:
                out["Wishlists (est.)"] = out["AppID"].apply(lambda a: fetch_wishlist_estimate_gamedata(storage, int(a)))
            except Exception:
                dbg["exceptions"] += 1
                st.session_state["last_exceptions"].append(
                    {
                        "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "stage": "wishlists",
                        "country": None,
                        "appid": None,
                        "error": "Failed fetching wishlist estimates",
                        "trace": traceback.format_exc(),
                    }
                )

        out["Reviews/day"] = pd.to_numeric(out["Reviews/day"], errors="coerce").fillna(0.0)
        out = out[out["Reviews/day"] >= float(min_reviews_per_day)]

        wl_sort = pd.to_numeric(out["Wishlists (est.)"], errors="coerce").fillna(-1)
        out["_wl_sort"] = wl_sort

        out = out.sort_values(
            ["Reviews/day", "_wl_sort", "% Positive", "Release Date"],
            ascending=[False, False, False, False],
        ).head(show_top_n)

        out = out.drop(columns=["_wl_sort"], errors="ignore")

        out["Followers"] = None
        if show_followers:
            try:
                out["Followers"] = out["AppID"].apply(lambda a: fetch_followers(storage, int(a)))
            except Exception:
                dbg["exceptions"] += 1
                st.session_state["last_exceptions"].append(
                    {
                        "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "stage": "followers",
                        "country": None,
                        "appid": None,
                        "error": "Failed fetching followers",
                        "trace": traceback.format_exc(),
                    }
                )

    else:
        first_fields = df.groupby("AppID").agg(
            {
                "Name": "first",
                "Store": "first",
                "Developer": "first",
                "Publisher": "first",
                "Release": "first",
                "Days Until": "min",
                "Genres/Categories": "first",
            }
        )
        out = pd.concat([first_fields, countries_agg], axis=1).reset_index()

        out["Wishlists (est.)"] = None
        if show_wishlists:
            try:
                out["Wishlists (est.)"] = out["AppID"].apply(lambda a: fetch_wishlist_estimate_gamedata(storage, int(a)))
            except Exception:
                dbg["exceptions"] += 1
                st.session_state["last_exceptions"].append(
                    {
                        "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "stage": "wishlists",
                        "country": None,
                        "appid": None,
                        "error": "Failed fetching wishlist estimates",
                        "trace": traceback.format_exc(),
                    }
                )

        out["_days_sort"] = pd.to_numeric(out["Days Until"], errors="coerce")
        out["_unknown"] = out["_days_sort"].isna().astype(int)

        out = out.sort_values(
            ["_unknown", "_days_sort", "Release"],
            ascending=[True, True, True],
        ).head(show_top_n)

        out = out.drop(columns=["_days_sort", "_unknown"], errors="ignore")

        out["Followers"] = None
        if show_followers:
            try:
                out["Followers"] = out["AppID"].apply(lambda a: fetch_followers(storage, int(a)))
            except Exception:
                dbg["exceptions"] += 1
                st.session_state["last_exceptions"].append(
                    {
                        "ts": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "stage": "followers",
                        "country": None,
                        "appid": None,
                        "error": "Failed fetching followers",
                        "trace": traceback.format_exc(),
                    }
                )

    display_df = out.drop(columns=["AppID"], errors="ignore")

    # Persist results for reruns (export dropdown changes etc.)
    run_date = datetime.now().strftime("%d-%m-%Y")
    mode_label = "New" if mode == "New releases" else "Upcoming"
    st.session_state["last_display_df"] = display_df
    st.session_state["last_mode_label"] = mode_label
    st.session_state["last_run_date"] = run_date
    st.session_state["last_dbg"] = dbg


# -----------------------------------------------------------------------------
# Render persisted results (so export dropdown never clears table)
# -----------------------------------------------------------------------------
display_df = st.session_state.get("last_display_df", None)
mode_label = st.session_state.get("last_mode_label", None)
run_date = st.session_state.get("last_run_date", None)
dbg = st.session_state.get("last_dbg", None)

if display_df is not None and mode_label is not None and run_date is not None:
    # Export (dropdown + single download button) from persisted results
    base_name = f"Steam Radar - {mode_label} {run_date}"

    export_format = st.selectbox(
        "Export format",
        ["Export as .csv", "Export as .xlsx"],
        index=0 if st.session_state["export_format"] == "Export as .csv" else 1,
        key="export_format",
    )

    if export_format == "Export as .csv":
        file_name = f"{base_name}.csv"
        data_bytes = display_df.to_csv(index=False).encode("utf-8")
        mime_type = "text/csv"
    else:
        file_name = f"{base_name}.xlsx"
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            display_df.to_excel(writer, index=False, sheet_name="Steam Radar")
        data_bytes = buffer.getvalue()
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    st.download_button(
        label="Download export",
        data=data_bytes,
        file_name=file_name,
        mime=mime_type,
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={"Store": st.column_config.LinkColumn("Store page")},
        hide_index=True,
    )

    if dbg:
        with st.expander("Why items were filtered (debug)"):
            st.write(dbg)

# -----------------------------------------------------------------------------
# Exceptions log (always at bottom)
# -----------------------------------------------------------------------------
exceptions = st.session_state.get("last_exceptions", []) or []
if exceptions:
    st.divider()
    st.subheader("Exceptions log")

    # compact table
    ex_df = pd.DataFrame(
        [
            {
                "Time": e.get("ts"),
                "Stage": e.get("stage"),
                "Country": e.get("country"),
                "AppID": e.get("appid"),
                "Error": e.get("error"),
            }
            for e in exceptions
        ]
    )
    st.dataframe(ex_df, use_container_width=True, hide_index=True)

    with st.expander("Full exception traces"):
        for i, e in enumerate(exceptions, start=1):
            st.markdown(f"**#{i} | {e.get('ts')} | {e.get('stage')} | {e.get('country')} | AppID={e.get('appid')}**")
            st.code(e.get("trace", ""), language="text")
