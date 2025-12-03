# app.py
"""
YouTube Viral Topics Tool - Premium Dashboard UI (No AI, No Telegram)
Features:
- Async fetching of YouTube search results using aiohttp
- Fetch video & channel statistics
- SQLite storage (daily insert/update)
- Duplicate filtering
- Viral score (views / (subs + 1))
- Streamlit dashboard: search UI, live results, saved history, charts
- CSV export
- Robust error handling and simple retry/backoff
"""

import asyncio
import aiohttp
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import pandas as pd
import math

import streamlit as st
from urllib.parse import urlencode
from matplotlib import pyplot as plt

# ---------------------------
# CONFIG - Put your API key here
# ---------------------------
YOUTUBE_API_KEY = "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU"  # <-- Replace with your API key
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"

DB_FILENAME = "youtube_data.db"  # created in same folder as app.py

# ---------------------------
# Database helpers
# ---------------------------
def init_db():
    conn = sqlite3.connect(DB_FILENAME, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        video_id TEXT PRIMARY KEY,
        keyword TEXT,
        title TEXT,
        description TEXT,
        thumbnail TEXT,
        views INTEGER,
        subscribers INTEGER,
        published_at TEXT,
        last_seen DATE
    )
    """)
    conn.commit()
    return conn

def upsert_video(conn: sqlite3.Connection, record: Dict[str, Any]):
    """
    Insert or update a video's record.
    If video_id exists -> update views, subscribers, last_seen, description (short), keyword if new.
    """
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO videos (video_id, keyword, title, description, thumbnail, views, subscribers, published_at, last_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        views = excluded.views,
        subscribers = excluded.subscribers,
        last_seen = excluded.last_seen,
        description = excluded.description,
        keyword = CASE WHEN instr(videos.keyword, excluded.keyword) = 0 THEN videos.keyword || ',' || excluded.keyword ELSE videos.keyword END
    """, (
        record["video_id"],
        record["keyword"],
        record["title"],
        record["description"],
        record["thumbnail"],
        record["views"],
        record["subscribers"],
        record.get("published_at", ""),
        record["last_seen"]
    ))
    conn.commit()

def fetch_all_saved(conn: sqlite3.Connection) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute("SELECT video_id, keyword, title, description, thumbnail, views, subscribers, published_at, last_seen FROM videos")
    rows = cur.fetchall()
    cols = ["video_id","keyword","title","description","thumbnail","views","subscribers","published_at","last_seen"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

# ---------------------------
# Utility functions
# ---------------------------
def short_text(text: str, limit: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= limit else text[:limit-3] + "..."

def compute_viral_score(views: int, subscribers: int) -> float:
    # Simple non-AI viral score
    return views / (subscribers + 1)

def iso_now_minus_days(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"

# ---------------------------
# Async YouTube fetchers
# ---------------------------
async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict, retries=2, backoff=1) -> dict:
    """
    Helper to GET JSON with basic retry/backoff.
    """
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=20) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status in (403, 429):
                    # quota or rate-limited: wait and retry
                    await asyncio.sleep(backoff * (attempt + 1))
                else:
                    text = await resp.text()
                    st.warning(f"HTTP {resp.status} for {url} - {text[:200]}")
                    await asyncio.sleep(1)
        except asyncio.TimeoutError:
            await asyncio.sleep(backoff * (attempt + 1))
        except Exception as e:
            await asyncio.sleep(1)
    return {}

async def search_videos_for_keyword(session: aiohttp.ClientSession, keyword: str, published_after: str, max_results: int=5) -> List[Dict[str,Any]]:
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "video",
        "order": "viewCount",
        "publishedAfter": published_after,
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY
    }
    data = await fetch_json(session, YOUTUBE_SEARCH_URL, params)
    items = data.get("items", [])
    return items

async def fetch_videos_stats(session: aiohttp.ClientSession, video_ids: List[str]) -> Dict[str, dict]:
    if not video_ids:
        return {}
    params = {
        "part": "statistics,contentDetails",
        "id": ",".join(video_ids),
        "key": YOUTUBE_API_KEY
    }
    data = await fetch_json(session, YOUTUBE_VIDEO_URL, params)
    stats = {}
    for item in data.get("items", []):
        vid = item.get("id")
        statistics = item.get("statistics", {})
        stats[vid] = statistics
    return stats

async def fetch_channels_stats(session: aiohttp.ClientSession, channel_ids: List[str]) -> Dict[str, dict]:
    if not channel_ids:
        return {}
    params = {
        "part": "statistics",
        "id": ",".join(channel_ids),
        "key": YOUTUBE_API_KEY
    }
    data = await fetch_json(session, YOUTUBE_CHANNEL_URL, params)
    ch_stats = {}
    for item in data.get("items", []):
        cid = item.get("id")
        statistics = item.get("statistics", {})
        ch_stats[cid] = statistics
    return ch_stats

# ---------------------------
# Core orchestration
# ---------------------------
async def process_keywords(keywords: List[str], days: int, max_results_per_keyword: int, min_views:int, max_subs:int, progress_callback=None) -> List[Dict[str,Any]]:
    """
    Search and fetch stats for all keywords in parallel.
    Returns list of video records.
    """
    published_after = iso_now_minus_days(days)
    connector = aiohttp.TCPConnector(limit=20)  # concurrency limit
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for kw in keywords:
            tasks.append(search_videos_for_keyword(session, kw, published_after, max_results_per_keyword))
        search_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten videos and keep mapping to keyword
        videos_flat = []
        for i, res in enumerate(search_results):
            kw = keywords[i]
            if isinstance(res, Exception):
                continue
            for item in res:
                # Validate presence of required keys
                snip = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId")
                if not video_id:
                    continue
                videos_flat.append({
                    "keyword": kw,
                    "video_id": video_id,
                    "snippet": snip
                })

        # Remove duplicates by video_id (keep first keyword)
        unique = {}
        for v in videos_flat:
            vid = v["video_id"]
            if vid not in unique:
                unique[vid] = v

        unique_list = list(unique.values())

        # Batch fetch stats in chunks (YouTube API supports multiple ids)
        results = []
        BATCH = 40  # safe chunk size for combined requests
        for start in range(0, len(unique_list), BATCH):
            batch = unique_list[start:start+BATCH]
            video_ids = [v["video_id"] for v in batch]
            channel_ids = [v["snippet"].get("channelId") for v in batch]
            # fetch stats concurrently
            video_stats = await fetch_videos_stats(session, video_ids)
            channel_stats = await fetch_channels_stats(session, channel_ids)

            # combine
            for v in batch:
                vid = v["video_id"]
                snip = v["snippet"]
                vs = video_stats.get(vid, {})
                cid = snip.get("channelId")
                cs = channel_stats.get(cid, {})

                try:
                    views = int(vs.get("viewCount", 0))
                except:
                    views = 0
                try:
                    subs = int(cs.get("subscriberCount", 0))
                except:
                    subs = 0

                record = {
                    "video_id": vid,
                    "keyword": v["keyword"],
                    "title": snip.get("title", "N/A"),
                    "description": short_text(snip.get("description","")),
                    "thumbnail": (snip.get("thumbnails") or {}).get("high",{}).get("url",""),
                    "views": views,
                    "subscribers": subs,
                    "published_at": snip.get("publishedAt",""),
                    "last_seen": datetime.utcnow().strftime("%Y-%m-%d")
                }

                # apply filters: min views and max subs
                if record["views"] >= min_views and record["subscribers"] <= max_subs:
                    results.append(record)

            if progress_callback:
                progress_callback(min(len(unique_list), start+BATCH), len(unique_list))

        return results

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="YouTube Viral Topics - Premium Dashboard", layout="wide")
st.title("YouTube Viral Topics — Premium Dashboard")
st.caption("Fast async search • SQLite history • Viral score • CSV export")

# Initialize DB
conn = init_db()

# Sidebar - Controls
with st.sidebar:
    st.header("Search Settings")
    days = st.slider("Days to search (past):", min_value=1, max_value=30, value=5)
    max_results = st.number_input("Max results per keyword:", min_value=1, max_value=50, value=5)
    min_views = st.number_input("Min views to consider:", min_value=0, value=100)
    max_subs = st.number_input("Max subscribers allowed:", min_value=0, value=3000)
    keywords_text = st.text_area("Keywords (one per line):", value="\n".join([
        "Affair Relationship Stories", "Reddit Relationship Advice", "Reddit Cheating",
        "AITA Update", "Open Marriage", "Cheating Story Real", "Reddit Marriage"
    ]), height=200)
    keywords = [k.strip() for k in keywords_text.splitlines() if k.strip()]
    st.markdown("---")
    st.subheader("Export / DB")
    if st.button("Download full DB as CSV"):
        df_all = fetch_all_saved(conn)
        csv = df_all.to_csv(index=False)
        st.download_button("Download CSV", data=csv, file_name="youtube_saved.csv", mime="text/csv")
    if st.button("Clear DB (Delete All)"):
        # ask confirmation
        confirm = st.checkbox("Confirm delete all data")
        if confirm:
            c = conn.cursor()
            c.execute("DELETE FROM videos")
            conn.commit()
            st.success("All data deleted from DB.")

# Main layout: two columns
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Live Search")
    st.write("API Key in file variable YOUTUBE_API_KEY; make sure it's set.")
    if YOUTUBE_API_KEY == "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU":
        st.error("You must set your YouTube API key in the script variable YOUTUBE_API_KEY before running searches.")
    days_input = days  # from sidebar
    max_results_input = max_results
    min_views_input = min_views
    max_subs_input = max_subs

    run_search = st.button("Fetch Data (Async)")

    if run_search and YOUTUBE_API_KEY != "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU":
        progress_text = st.empty()
        prog_bar = st.progress(0)
        status_box = st.empty()
        results_container = st.container()

        async def run_and_display():
            start_time = time.time()
            status_box.info("Starting async search...")
            # callback to update progress
            def progress_cb(done, total):
                try:
                    frac = done / total if total else 1
                    prog_bar.progress(min(100, math.floor(frac * 100)))
                    progress_text.text(f"Processed {done} of {total} unique videos (batches)")
                except:
                    pass

            try:
                results = await process_keywords(
                    keywords=keywords,
                    days=days_input,
                    max_results_per_keyword=max_results_input,
                    min_views=min_views_input,
                    max_subs=max_subs_input,
                    progress_callback=progress_cb
                )
            except Exception as e:
                status_box.error(f"Error during fetching: {e}")
                return []

            elapsed = time.time() - start_time
            status_box.success(f"Fetched {len(results)} filtered results in {elapsed:.1f}s")
            prog_bar.progress(100)
            return results

        # Run asyncio in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(run_and_display())

        # Display & Save
        if results:
            # convert to DataFrame for sorting/display
            df = pd.DataFrame(results)
            df["viral_score"] = df.apply(lambda r: compute_viral_score(r["views"], r["subscribers"]), axis=1)
            df = df.sort_values(by="viral_score", ascending=False).reset_index(drop=True)

            st.markdown("### Results (filtered & scored)")
            # Show table with thumbnails as cards
            for idx, row in df.iterrows():
                with results_container:
                    cols = st.columns([1, 3, 1])
                    with cols[0]:
                        if row["thumbnail"]:
                            st.image(row["thumbnail"], width=160)
                    with cols[1]:
                        st.markdown(f"**{row['title']}**  \nKeyword: `{row['keyword']}`  \nViews: **{row['views']}**  • Subs: **{row['subscribers']}**  • Viral Score: **{row['viral_score']:.2f}**")
                        st.write(short_text(row["description"], 300))
                        st.markdown(f"[Watch Video](https://www.youtube.com/watch?v={row['video_id']})")
                    with cols[2]:
                        if st.button(f"Save ▶ {row['video_id']}", key=f"save_{row['video_id']}"):
                            upsert_video(conn, {
                                "video_id": row["video_id"],
                                "keyword": row["keyword"],
                                "title": row["title"],
                                "description": row["description"],
                                "thumbnail": row["thumbnail"],
                                "views": row["views"],
                                "subs": row["subscribers"],
                                "published_at": row["published_at"],
                                "last_seen": row["last_seen"]
                            })
                            st.success("Saved to DB")

            # Show CSV download for current results
            csv = df.to_csv(index=False)
            st.download_button("Download Current Results as CSV", data=csv, file_name="current_results.csv", mime="text/csv")
        else:
            st.warning("No results matched the filters (min_views / max_subs).")

with col2:
    st.subheader("History Dashboard (Saved Data)")
    df_saved = fetch_all_saved(conn)
    st.markdown(f"**Total saved videos:** {len(df_saved)}")
    if not df_saved.empty:
        # Viral score column
        df_saved["viral_score"] = df_saved.apply(lambda r: compute_viral_score(int(r["views"]), int(r["subscribers"])), axis=1)
        # Top keywords by count
        # keyword column may contain comma-separated keywords - explode them
        exploded = df_saved.assign(keyword_exploded = df_saved["keyword"].str.split(",")).explode("keyword_exploded")
        kw_counts = exploded["keyword_exploded"].value_counts().rename_axis("keyword").reset_index(name="count")
        st.markdown("**Top Keywords (by saved videos)**")
        st.table(kw_counts.head(10))

        # Chart 1: Views distribution (matplotlib)
        st.markdown("**Views Distribution (Saved Videos)**")
        fig1 = plt.figure(figsize=(5,3))
        plt.hist(df_saved["views"].astype(int), bins=20)
        plt.xlabel("Views")
        plt.ylabel("Count")
        plt.tight_layout()
        st.pyplot(fig1)

        # Chart 2: Viral Score top 10
        st.markdown("**Top 10 by Viral Score**")
        top_vs = df_saved.sort_values(by="viral_score", ascending=False).head(10)
        fig2 = plt.figure(figsize=(5,3))
        plt.bar(range(len(top_vs)), top_vs["viral_score"].astype(float))
        plt.xticks(range(len(top_vs)), top_vs["title"].str.slice(0,30).tolist(), rotation=45, ha="right")
        plt.ylabel("Viral Score")
        plt.tight_layout()
        st.pyplot(fig2)

        # Table of saved videos with quick actions
        st.markdown("**Saved Videos (table)**")
        display_df = df_saved[["video_id","title","views","subscribers","last_seen","viral_score"]].sort_values(by="viral_score", ascending=False)
        st.dataframe(display_df)

        # Option: Show time series for a specific video (if multiple dates saved - but our schema keeps only last_seen and last stats)
        st.markdown("**Quick Insights**")
        avg_views = int(df_saved["views"].astype(int).mean())
        avg_subs = int(df_saved["subscribers"].astype(int).mean())
        st.write(f"Average views among saved videos: **{avg_views}**")
        st.write(f"Average subscribers among saved channels: **{avg_subs}**")

    else:
        st.info("No saved videos yet. Use the 'Fetch Data' panel, then 'Save' buttons to store results.")

# Footer / Logs
st.markdown("---")
st.caption("Built with YouTube Data API • SQLite • Streamlit • aiohttp")

   
