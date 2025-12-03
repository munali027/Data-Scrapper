import asyncio
import aiohttp
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

import streamlit as st
from matplotlib import pyplot as plt

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------
YOUTUBE_API_KEY = "AIzaSyBxHc-_agIj6Wf_x3hJdP5cW-3m9UXshJU"
DB_FILENAME = "youtube_data.db"

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"


# ----------------------------------------------------------
# DATABASE
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


def upsert_video(conn, record):
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
        record["subscribers"],
        record["published_at"],
        record["last_seen"]
    ))
    conn.commit()


def fetch_all_saved(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT video_id, keyword, title, description, thumbnail,
               views, subscribers, published_at, last_seen
        FROM videos
        ORDER BY last_seen DESC
    """)
    rows = cur.fetchall()
    cols = ["video_id", "keyword", "title", "description", "thumbnail",
            "views", "subscribers", "published_at", "last_seen"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def delete_video(conn, video_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM videos WHERE video_id=?", (video_id,))
    conn.commit()


def remove_keyword_from_video(conn, video_id, keyword_to_remove):
    cur = conn.cursor()
    cur.execute("SELECT keyword FROM videos WHERE video_id=?", (video_id,))
    row = cur.fetchone()
    if not row:
        return
    current = row[0] or ""
    keywords = [k.strip() for k in current.split(",") if k.strip()]
    keywords = [k for k in keywords if k != keyword_to_remove]
    updated = ",".join(keywords)
    cur.execute("UPDATE videos SET keyword=? WHERE video_id=?", (updated, video_id))
    conn.commit()


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------
def short_text(text, limit=200):
    if not text:
        return ""
    return text if len(text) <= limit else text[:limit - 3] + "..."


def compute_viral_score(views, subscribers):
    try:
        return views / (subscribers + 1)
    except Exception:
        return float(views)


def iso_now_minus_days(days):
    return (datetime.utcnow() - timedelta(days=days)).isoformat("T") + "Z"


# ----------------------------------------------------------
# ASYNC API FUNCTIONS (supporting regionCode)
# ----------------------------------------------------------
async def fetch_json(session, url, params, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=15) as resp:
                if resp.status == 200:
                    return await resp.json()
            await asyncio.sleep(1)
        except:
            await asyncio.sleep(1)
    return {}


async def search_videos(session, keyword, published_after, max_results, region_code=None):
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "video",
        "order": "viewCount",
        "publishedAfter": published_after,
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY
    }
    if region_code:
        params["regionCode"] = region_code
    return await fetch_json(session, YOUTUBE_SEARCH_URL, params)


async def fetch_video_stats(session, video_ids):
    if not video_ids:
        return {}
    params = {"part": "statistics", "id": ",".join(video_ids), "key": YOUTUBE_API_KEY}
    data = await fetch_json(session, YOUTUBE_VIDEO_URL, params)
    out = {}
    for item in data.get("items", []):
        out[item.get("id")] = item.get("statistics", {})
    return out


async def fetch_channel_stats(session, channel_ids):
    if not channel_ids:
        return {}
    params = {"part": "statistics", "id": ",".join(channel_ids), "key": YOUTUBE_API_KEY}
    data = await fetch_json(session, YOUTUBE_CHANNEL_URL, params)
    out = {}
    for item in data.get("items", []):
        out[item.get("id")] = item.get("statistics", {})
    return out


# ----------------------------------------------------------
# MAIN COLLECTOR
# ----------------------------------------------------------
async def process_keywords(keywords, days, max_results_per_kw, min_views, max_subs, region_code=None, progress_cb=None):
    published_after = iso_now_min_days(days)
    connector = aiohttp.TCPConnector(limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [search_videos(session, kw, published_after, max_results_per_kw, region_code) for kw in keywords]
        responses = await asyncio.gather(*tasks)

        videos_flat = []
        for i, data in enumerate(responses):
            kw = keywords[i]
            for item in data.get("items", []):
                vid = item.get("id", {}).get("videoId")
                if vid:
                    videos_flat.append({
                        "keyword": kw,
                        "video_id": vid,
                        "snippet": item["snippet"]
                    })

        # Deduplicate
        unique = {v["video_id"]: v for v in videos_flat}
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

                try:
                    views = int(video_stats.get(vid, {}).get("viewCount", 0) or 0)
                except:
                    views = 0
                try:
                    subs = int(channel_stats.get(snip["channelId"], {}).get("subscriberCount", 0) or 0)
                except:
                    subs = 0

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
                progress_cb(min(start + len(batch), len(unique_list)), len(unique_list))

        return results


# ----------------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------------
st.set_page_config(page_title="YouTube Viral Topics — Dashboard", layout="wide")
conn = init_db()

st.title("YouTube Viral Topics — Premium Dashboard")
st.caption("Async API search • Viral Finder • SQLite history • CSV export")

# session_state for persistence
if "results" not in st.session_state:
    st.session_state["results"] = []
if "last_fetch_meta" not in st.session_state:
    st.session_state["last_fetch_meta"] = {"count": 0, "country": None, "keywords": []}

# SIDEBAR: Filters + Country selector
with st.sidebar:
    st.header("Search Filters")

    days = st.slider("Days to search", 1, 30, 5)
    max_results = st.number_input("Max results per keyword", 1, 50, 5)
    min_views = st.number_input("Minimum Views", 0, 5000000, 100)
    max_subs = st.number_input("Maximum Channel Subscribers", 0, 10000000, 3000)

    # Country selector (YouTube region codes)
    country = st.selectbox(
        "Select Country (regionCode)",
        countries = [
    ("Afghanistan", "AF"),
    ("Albania", "AL"),
    ("Algeria", "DZ"),
    ("American Samoa", "AS"),
    ("Andorra", "AD"),
    ("Angola", "AO"),
    ("Anguilla", "AI"),
    ("Antarctica", "AQ"),
    ("Antigua and Barbuda", "AG"),
    ("Argentina", "AR"),
    ("Armenia", "AM"),
    ("Aruba", "AW"),
    ("Australia", "AU"),
    ("Austria", "AT"),
    ("Azerbaijan", "AZ"),
    ("Bahamas", "BS"),
    ("Bahrain", "BH"),
    ("Bangladesh", "BD"),
    ("Barbados", "BB"),
    ("Belarus", "BY"),
    ("Belgium", "BE"),
    ("Belize", "BZ"),
    ("Benin", "BJ"),
    ("Bermuda", "BM"),
    ("Bhutan", "BT"),
    ("Bolivia", "BO"),
    ("Bosnia and Herzegovina", "BA"),
    ("Botswana", "BW"),
    ("Brazil", "BR"),
    ("British Indian Ocean Territory", "IO"),
    ("Brunei Darussalam", "BN"),
    ("Bulgaria", "BG"),
    ("Burkina Faso", "BF"),
    ("Burundi", "BI"),
    ("Cambodia", "KH"),
    ("Cameroon", "CM"),
    ("Canada", "CA"),
    ("Cape Verde", "CV"),
    ("Cayman Islands", "KY"),
    ("Central African Republic", "CF"),
    ("Chad", "TD"),
    ("Chile", "CL"),
    ("China", "CN"),
    ("Christmas Island", "CX"),
    ("Cocos (Keeling) Islands", "CC"),
    ("Colombia", "CO"),
    ("Comoros", "KM"),
    ("Congo", "CG"),
    ("Cook Islands", "CK"),
    ("Costa Rica", "CR"),
    ("Croatia", "HR"),
    ("Cuba", "CU"),
    ("Cyprus", "CY"),
    ("Czech Republic", "CZ"),
    ("Denmark", "DK"),
    ("Djibouti", "DJ"),
    ("Dominica", "DM"),
    ("Dominican Republic", "DO"),
    ("Ecuador", "EC"),
    ("Egypt", "EG"),
    ("El Salvador", "SV"),
    ("Equatorial Guinea", "GQ"),
    ("Eritrea", "ER"),
    ("Estonia", "EE"),
    ("Ethiopia", "ET"),
    ("Falkland Islands", "FK"),
    ("Faroe Islands", "FO"),
    ("Fiji", "FJ"),
    ("Finland", "FI"),
    ("France", "FR"),
    ("French Guiana", "GF"),
    ("French Polynesia", "PF"),
    ("Gabon", "GA"),
    ("Gambia", "GM"),
    ("Georgia", "GE"),
    ("Germany", "DE"),
    ("Ghana", "GH"),
    ("Gibraltar", "GI"),
    ("Greece", "GR"),
    ("Greenland", "GL"),
    ("Grenada", "GD"),
    ("Guadeloupe", "GP"),
    ("Guam", "GU"),
    ("Guatemala", "GT"),
    ("Guernsey", "GG"),
    ("Guinea", "GN"),
    ("Guinea-Bissau", "GW"),
    ("Guyana", "GY"),
    ("Haiti", "HT"),
    ("Honduras", "HN"),
    ("Hong Kong", "HK"),
    ("Hungary", "HU"),
    ("Iceland", "IS"),
    ("India", "IN"),
    ("Indonesia", "ID"),
    ("Iran", "IR"),
    ("Iraq", "IQ"),
    ("Ireland", "IE"),
    ("Isle of Man", "IM"),
    ("Israel", "IL"),
    ("Italy", "IT"),
    ("Jamaica", "JM"),
    ("Japan", "JP"),
    ("Jersey", "JE"),
    ("Jordan", "JO"),
    ("Kazakhstan", "KZ"),
    ("Kenya", "KE"),
    ("Kiribati", "KI"),
    ("Korea (South)", "KR"),
    ("Kuwait", "KW"),
    ("Kyrgyzstan", "KG"),
    ("Lao PDR", "LA"),
    ("Latvia", "LV"),
    ("Lebanon", "LB"),
    ("Lesotho", "LS"),
    ("Liberia", "LR"),
    ("Libya", "LY"),
    ("Liechtenstein", "LI"),
    ("Lithuania", "LT"),
    ("Luxembourg", "LU"),
    ("Macao", "MO"),
    ("Macedonia", "MK"),
    ("Madagascar", "MG"),
    ("Malawi", "MW"),
    ("Malaysia", "MY"),
    ("Maldives", "MV"),
    ("Mali", "ML"),
    ("Malta", "MT"),
    ("Marshall Islands", "MH"),
    ("Martinique", "MQ"),
    ("Mauritania", "MR"),
    ("Mauritius", "MU"),
    ("Mayotte", "YT"),
    ("Mexico", "MX"),
    ("Micronesia", "FM"),
    ("Moldova", "MD"),
    ("Monaco", "MC"),
    ("Mongolia", "MN"),
    ("Montenegro", "ME"),
    ("Montserrat", "MS"),
    ("Morocco", "MA"),
    ("Mozambique", "MZ"),
    ("Myanmar", "MM"),
    ("Namibia", "NA"),
    ("Nauru", "NR"),
    ("Nepal", "NP"),
    ("Netherlands", "NL"),
    ("New Caledonia", "NC"),
    ("New Zealand", "NZ"),
    ("Nicaragua", "NI"),
    ("Niger", "NE"),
    ("Nigeria", "NG"),
    ("Niue", "NU"),
    ("Norfolk Island", "NF"),
    ("Northern Mariana Islands", "MP"),
    ("Norway", "NO"),
    ("Oman", "OM"),
    ("Pakistan", "PK"),
    ("Palau", "PW"),
    ("Panama", "PA"),
    ("Papua New Guinea", "PG"),
    ("Paraguay", "PY"),
    ("Peru", "PE"),
    ("Philippines", "PH"),
    ("Poland", "PL"),
    ("Portugal", "PT"),
    ("Puerto Rico", "PR"),
    ("Qatar", "QA"),
    ("Réunion", "RE"),
    ("Romania", "RO"),
    ("Russia", "RU"),
    ("Rwanda", "RW"),
    ("Saint Helena", "SH"),
    ("Saint Kitts and Nevis", "KN"),
    ("Saint Lucia", "LC"),
    ("Saint Pierre and Miquelon", "PM"),
    ("Saint Vincent", "VC"),
    ("Samoa", "WS"),
    ("San Marino", "SM"),
    ("Saudi Arabia", "SA"),
    ("Senegal", "SN"),
    ("Serbia", "RS"),
    ("Seychelles", "SC"),
    ("Sierra Leone", "SL"),
    ("Singapore", "SG"),
    ("Slovakia", "SK"),
    ("Slovenia", "SI"),
    ("Solomon Islands", "SB"),
    ("Somalia", "SO"),
    ("South Africa", "ZA"),
    ("Spain", "ES"),
    ("Sri Lanka", "LK"),
    ("Sudan", "SD"),
    ("Suriname", "SR"),
    ("Swaziland", "SZ"),
    ("Sweden", "SE"),
    ("Switzerland", "CH"),
    ("Syrian Arab Republic", "SY"),
    ("Taiwan", "TW"),
    ("Tajikistan", "TJ"),
    ("Tanzania", "TZ"),
    ("Thailand", "TH"),
    ("Timor-Leste", "TL"),
    ("Togo", "TG"),
    ("Tonga", "TO"),
    ("Trinidad and Tobago", "TT"),
    ("Tunisia", "TN"),
    ("Turkey", "TR"),
    ("Turkmenistan", "TM"),
    ("Tuvalu", "TV"),
    ("Uganda", "UG"),
    ("Ukraine", "UA"),
    ("United Arab Emirates", "AE"),
    ("United Kingdom", "GB"),
    ("United States of America", "US"),
    ("Uruguay", "UY"),
    ("Uzbekistan", "UZ"),
    ("Vanuatu", "VU"),
    ("Vatican City", "VA"),
    ("Venezuela", "VE"),
    ("Vietnam", "VN"),
    ("Virgin Islands (U.S.)", "VI"),
    ("Wallis and Futuna", "WF"),
    ("Western Sahara", "EH"),
    ("Yemen", "YE"),
    ("Zambia", "ZM"),
    ("Zimbabwe", "ZW"),
],
        index=0
    )

    keywords_text = st.text_area("Keywords (one per line)", value="Reddit Cheating\nAITA\nAffair Story")
    keywords = [k.strip() for k in keywords_text.split("\n") if k.strip()]

    st.write("---")

    if st.button("Download Saved CSV"):
        df_all = fetch_all_saved(conn)
        st.download_button("Download", df_all.to_csv(index=False), "saved_videos.csv")

    if st.button("Clear Database"):
        cur = conn.cursor()
        cur.execute("DELETE FROM videos")
        conn.commit()
        st.success("Database cleared")


# MAIN LAYOUT
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Live Search")

    # Fetch data button
    if st.button("Fetch Data (Async)"):
        if not keywords:
            st.warning("Add at least one keyword.")
        else:
            progress = st.progress(0)
            text = st.empty()

            def update_progress(done, total):
                pct = int(done / total * 100) if total else 0
                progress.progress(pct)
                text.text(f"Processed {done}/{total}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                process_keywords(
                    keywords, days, max_results,
                    min_views, max_subs,
                    region_code=country if country else None,
                    progress_cb=update_progress
                )
            )

            st.session_state["results"] = results
            st.session_state["last_fetch_meta"] = {
                "count": len(results),
                "country": country,
                "keywords": keywords
            }
            st.success(f"Found {len(results)} videos")

    # Show last fetch metadata
    meta = st.session_state.get("last_fetch_meta", {})
    if meta.get("count", 0):
        st.markdown(f"**Last fetch:** {meta.get('count')} results — Country: `{meta.get('country') or 'All'}` — Keywords: `{', '.join(meta.get('keywords', []))}`")

    # Display results from session_state
    if st.session_state["results"]:
        df = pd.DataFrame(st.session_state["results"])
        df["viral_score"] = df.apply(lambda r: compute_viral_score(int(r["views"]), int(r["subscribers"])), axis=1)
        df = df.sort_values("viral_score", ascending=False)

        for _, row in df.iterrows():
            c1, c2 = st.columns([1, 3])

            with c1:
                if row.get("thumbnail"):
                    st.image(row["thumbnail"], width=150)

            with c2:
                st.markdown(f"### {row.get('title')}")
                st.write(f"Keyword: `{row.get('keyword')}`")
                st.write(f"Views: **{row.get('views')}**, Subs: **{row.get('subscribers')}**, Score: **{row.get('viral_score'):.2f}**")
                st.write(short_text(row.get("description", "")))

                # Save button (no rerun)
                if st.button(f"Save {row.get('video_id')}", key=f"save_{row.get('video_id')}"):
                    # upsert and show success; session_state retains results so UI doesn't "lose" them
                    upsert_video(conn, row.to_dict())
                    st.success("Saved!")

with col2:
    st.subheader("Saved Videos Dashboard")

    df_saved = fetch_all_saved(conn)
    st.write(f"Total saved videos: {len(df_saved)}")

    if len(df_saved):
        df_saved["viral_score"] = df_saved.apply(lambda r: compute_viral_score(int(r["views"]), int(r["subscribers"])), axis=1)

        st.markdown("**Top Keywords**")
        exp = df_saved.assign(kw=df_saved["keyword"].str.split(",")).explode("kw")
        st.table(exp["kw"].value_counts().head(10))

        st.markdown("**Saved Videos List**")
        # Show each saved video with options to delete or remove keywords
        for _, row in df_saved.iterrows():
            vid = row["video_id"]
            title = row["title"]
            thumbs = row["thumbnail"]
            keywords_str = row["keyword"] or ""
            keywords_list = [k.strip() for k in keywords_str.split(",") if k.strip()]

            with st.expander(f"{title} ({vid})", expanded=False):
                c1, c2 = st.columns([1, 3])
                with c1:
                    if thumbs:
                        st.image(thumbs, width=120)
                with c2:
                    st.write(f"Views: **{row['views']}**, Subs: **{row['subscribers']}**, Score: **{compute_viral_score(int(row['views']), int(row['subscribers'])):.2f}**")
                    st.write(short_text(row["description"]))
                    st.write("---")
                    st.write("**Keywords attached:**")
                    if keywords_list:
                        kw_cols = st.columns(len(keywords_list))
                        for i, kw in enumerate(keywords_list):
                            with kw_cols[i]:
                                st.write(f"`{kw}`")
                                # Remove specific keyword button
                                if st.button(f"Remove '{kw}' from {vid}", key=f"rem_{vid}_{kw}"):
                                    remove_keyword_from_video(conn, vid, kw)
                                    st.success(f"Removed keyword '{kw}'")
                                    # refresh df_saved after action
                                    df_saved = fetch_all_saved(conn)
                                    st.experimental_rerun()
                    else:
                        st.write("_No keywords attached_")

                    st.write("---")
                    # Delete whole video
                    if st.button(f"Delete video {vid}", key=f"del_{vid}"):
                        delete_video(conn, vid)
                        st.success("Video deleted from saved list.")
                        st.experimental_rerun()

        # Top viral plot
        st.markdown("**Top Viral Videos**")
        top = df_saved.sort_values("viral_score", ascending=False).head(10)
        fig = plt.figure(figsize=(5, 3))
        plt.bar(range(len(top)), top["viral_score"])
        plt.xticks(range(len(top)), top["title"].str.slice(0, 20), rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
