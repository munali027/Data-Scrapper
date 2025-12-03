import asyncio
import aiohttp
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
import math

import streamlit as st
from matplotlib import pyplot as plt


# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------
YOUTUBE_API_KEY = "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU"   # <-- REQUIRED!
DB_FILENAME = "youtube_data.db"

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"


# ----------------------------------------------------------
# DATABASE SETUP
# ----------------------------------------------------------
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
        last_seen TEXT
    )
    """)
    conn.commit()
    return conn


def upsert_video(conn: sqlite3.Connection, record: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO videos (video_id, keyword, title, description, thumbnail, views, subscribers, published_at, last_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        views = excluded.views,
        subscribers = excluded.subscribers,
        last_seen = excluded.last_seen,
        description = excluded.description,
        keyword = CASE
            WHEN instr(videos.keyword, excluded.keyword) = 0
            THEN videos.keyword || ',' || excluded.keyword
            ELSE videos.keyword
        END
    """, (
        record["video_id"],
        record["keyword"],
        record["title"],
        record["description"],
        record["thumbnail"],
        record["views"],
        record["subscribers"],     # FIXED NAME
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


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------
def short_text(text: str, limit: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= limit else text[:limit - 3] + "..."


def compute_viral_score(views: int, subscribers: int) -> float:
    return views / (subscribers + 1)


def iso_now_minus_days(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"


# ----------------------------------------------------------
# ASYNC FETCH FUNCTIONS
# ----------------------------------------------------------
async def fetch_json(session, url, params, retries=2):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=20) as resp:
                if resp.status == 200:
                    return await resp.json()
                await asyncio.sleep(1 * (attempt + 1))
        except:
            await asyncio.sleep(1)
    return {}


async def search_videos(session, keyword, published_after, max_results):
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "video",
        "order": "viewCount",
        "publishedAfter": published_after,
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY
    }
    return await fetch_json(session, YOUTUBE_SEARCH_URL, params)


async def fetch_video_stats(session, video_ids):
    if not video_ids:
        return {}
    params = {"part": "statistics", "id": ",".join(video_ids), "key": YOUTUBE_API_KEY}
    data = await fetch_json(session, YOUTUBE_VIDEO_URL, params)
    out = {}
    for item in data.get("items", []):
        out[item["id"]] = item.get("statistics", {})
    return out


async def fetch_channel_stats(session, channel_ids):
    if not channel_ids:
        return {}
    params = {"part": "statistics", "id": ",".join(channel_ids), "key": YOUTUBE_API_KEY}
    data = await fetch_json(session, YOUTUBE_CHANNEL_URL, params)
    out = {}
    for item in data.get("items", []):
        out[item["id"]] = item.get("statistics", {})
    return out


# ----------------------------------------------------------
# MAIN ASYNC PROCESSOR
# ----------------------------------------------------------
async def process_keywords(keywords, days, max_results_per_kw, min_views, max_subs, progress_cb=None):
    published_after = iso_now_minus_days(days)
    connector = aiohttp.TCPConnector(limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [search_videos(session, kw, published_after, max_results_per_kw) for kw in keywords]
        responses = await asyncio.gather(*tasks)

        # Flatten videos
        videos_flat = []
        for i, data in enumerate(responses):
            kw = keywords[i]
            items = data.get("items", [])
            for item in items:
                vid = item.get("id", {}).get("videoId")
                if vid:
                    videos_flat.append({
                        "keyword": kw,
                        "video_id": vid,
                        "snippet": item["snippet"]
                    })

        # Unique video IDs
        unique = {}
        for v in videos_flat:
            if v["video_id"] not in unique:
                unique[v["video_id"]] = v

        unique_list = list(unique.values())

        results = []
        batch_size = 40

        for start in range(0, len(unique_list), batch_size):
            batch = unique_list[start:start + batch_size]
            video_ids = [v["video_id"] for v in batch]
            channel_ids = [v["snippet"]["channelId"] for v in batch]

            video_stats = await fetch_video_stats(session, video_ids)
            channel_stats = await fetch_channel_stats(session, channel_ids)

            for v in batch:
                vid = v["video_id"]
                snip = v["snippet"]

                views = int(video_stats.get(vid, {}).get("viewCount", 0))
                subs = int(channel_stats.get(snip["channelId"], {}).get("subscriberCount", 0))

                rec = {
                    "video_id": vid,
                    "keyword": v["keyword"],
                    "title": snip.get("title", "N/A"),
                    "description": short_text(snip.get("description", "")),
                    "thumbnail": snip.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "views": views,
                    "subscribers": subs,
                    "published_at": snip.get("publishedAt", ""),
                    "last_seen": datetime.utcnow().strftime("%Y-%m-%d")
                }

                if views >= min_views and subs <= max_subs:
                    results.append(rec)

            if progress_cb:
                progress_cb(start + len(batch), len(unique_list))

        return results


# ----------------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------------
st.set_page_config(page_title="YouTube Viral Topics — Dashboard", layout="wide")
conn = init_db()

st.title("YouTube Viral Topics — Premium Dashboard")
st.caption("Async API search • SQLite history • Viral score • CSV export")


# Sidebar
with st.sidebar:
    st.header("Search Settings")
    days = st.slider("Days to search", 1, 30, 5)
    max_results = st.number_input("Max results per keyword", 1, 50, 5)
    min_views = st.number_input("Min Views", 0, 1000000, 100)
    max_subs = st.number_input("Max Subscribers", 0, 10000000, 3000)

    keywords_text = st.text_area("Keywords", value="Reddit Cheating\nAITA\nAffair Story")
    keywords = [k.strip() for k in keywords_text.split("\n") if k.strip()]

    st.write("---")

    if st.button("Download Saved as CSV"):
        df_all = fetch_all_saved(conn)
        st.download_button("Download File", df_all.to_csv(index=False), "saved_data.csv")

    if st.button("Clear Database"):
        c = conn.cursor()
        c.execute("DELETE FROM videos")
        conn.commit()
        st.success("Database cleared")


# MAIN UI
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Live Search")

    if YOUTUBE_API_KEY == "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU":
        st.error("Add your YouTube API key inside app.py first!")
    else:
        run = st.button("Fetch Data (Async)")

        if run:
            progress = st.progress(0)
            text = st.empty()

            def update_progress(done, total):
                pct = int((done / total) * 100)
                progress.progress(pct)
                text.text(f"Processed {done}/{total}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                process_keywords(
                    keywords, days, max_results, min_views, max_subs,
                    progress_cb=update_progress
                )
            )

            st.success(f"Found {len(results)} videos")

            if results:
                df = pd.DataFrame(results)
                df["viral_score"] = df.apply(lambda r: compute_viral_score(r["views"], r["subscribers"]), axis=1)
                df = df.sort_values("viral_score", ascending=False)

                for _, row in df.iterrows():
                    c1, c2 = st.columns([1, 3])

                    with c1:
                        if row["thumbnail"]:
                            st.image(row["thumbnail"], width=150)

                    with c2:
                        st.markdown(f"### {row['title']}")
                        st.write(f"Keyword: `{row['keyword']}`")
                        st.write(f"Views: **{row['views']}**, Subs: **{row['subscribers']}**, Score: **{row['viral_score']:.2f}**")
                        st.write(short_text(row["description"]))

                        if st.button(f"Save {row['video_id']}", key=row["video_id"]):
                            upsert_video(conn, row.to_dict())
                            st.success("Saved!")


with col2:
    st.subheader("History Dashboard (Saved Data)")
    df_saved = fetch_all_saved(conn)

    st.write(f"Total saved videos: {len(df_saved)}")

    if len(df_saved):
        df_saved["viral_score"] = df_saved.apply(lambda r: compute_viral_score(int(r["views"]), int(r["subscribers"])), axis=1)

        st.markdown("**Top Keywords**")
        exp = df_saved.assign(kw=df_saved["keyword"].str.split(",")).explode("kw")
        st.table(exp["kw"].value_counts().head(10))

        st.markdown("**Top 10 Viral Score**")
        top = df_saved.sort_values("viral_score", ascending=False).head(10)

        fig = plt.figure(figsize=(5, 3))
        plt.bar(range(len(top)), top["viral_score"])
        plt.xticks(range(len(top)), top["title"].str.slice(0, 20), rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
