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


# ----------------------------------------------------------
# FULL WORLD COUNTRY LIST (NAME + CODE)
# ----------------------------------------------------------
COUNTRIES = [
    ("Select Country (Global Search)", ""),
    ("Afghanistan", "AF"), ("Albania", "AL"), ("Algeria", "DZ"),
    ("American Samoa", "AS"), ("Andorra", "AD"), ("Angola", "AO"),
    ("Anguilla", "AI"), ("Antarctica", "AQ"), ("Antigua and Barbuda", "AG"),
    ("Argentina", "AR"), ("Armenia", "AM"), ("Aruba", "AW"),
    ("Australia", "AU"), ("Austria", "AT"), ("Azerbaijan", "AZ"),
    ("Bahamas", "BS"), ("Bahrain", "BH"), ("Bangladesh", "BD"),
    ("Barbados", "BB"), ("Belarus", "BY"), ("Belgium", "BE"),
    ("Belize", "BZ"), ("Benin", "BJ"), ("Bermuda", "BM"),
    ("Bhutan", "BT"), ("Bolivia", "BO"), ("Bosnia and Herzegovina", "BA"),
    ("Botswana", "BW"), ("Brazil", "BR"), ("British Indian Ocean Territory", "IO"),
    ("Brunei Darussalam", "BN"), ("Bulgaria", "BG"), ("Burkina Faso", "BF"),
    ("Burundi", "BI"), ("Cambodia", "KH"), ("Cameroon", "CM"),
    ("Canada", "CA"), ("Cape Verde", "CV"), ("Cayman Islands", "KY"),
    ("Central African Republic", "CF"), ("Chad", "TD"), ("Chile", "CL"),
    ("China", "CN"), ("Christmas Island", "CX"), ("Cocos (Keeling) Islands", "CC"),
    ("Colombia", "CO"), ("Comoros", "KM"), ("Congo", "CG"),
    ("Cook Islands", "CK"), ("Costa Rica", "CR"), ("Croatia", "HR"),
    ("Cuba", "CU"), ("Cyprus", "CY"), ("Czech Republic", "CZ"),
    ("Denmark", "DK"), ("Djibouti", "DJ"), ("Dominica", "DM"),
    ("Dominican Republic", "DO"), ("Ecuador", "EC"), ("Egypt", "EG"),
    ("El Salvador", "SV"), ("Equatorial Guinea", "GQ"), ("Eritrea", "ER"),
    ("Estonia", "EE"), ("Ethiopia", "ET"), ("Fiji", "FJ"), ("Finland", "FI"),
    ("France", "FR"), ("Gabon", "GA"), ("Gambia", "GM"),
    ("Georgia", "GE"), ("Germany", "DE"), ("Ghana", "GH"),
    ("Greece", "GR"), ("Greenland", "GL"), ("Grenada", "GD"),
    ("Guatemala", "GT"), ("Guernsey", "GG"), ("Guinea", "GN"),
    ("Guyana", "GY"), ("Haiti", "HT"), ("Honduras", "HN"),
    ("Hong Kong", "HK"), ("Hungary", "HU"), ("Iceland", "IS"),
    ("India", "IN"), ("Indonesia", "ID"), ("Iran", "IR"), ("Iraq", "IQ"),
    ("Ireland", "IE"), ("Isle of Man", "IM"), ("Israel", "IL"), ("Italy", "IT"),
    ("Jamaica", "JM"), ("Japan", "JP"), ("Jordan", "JO"), ("Kazakhstan", "KZ"),
    ("Kenya", "KE"), ("Kiribati", "KI"), ("South Korea", "KR"),
    ("Kuwait", "KW"), ("Kyrgyzstan", "KG"), ("Laos", "LA"),
    ("Latvia", "LV"), ("Lebanon", "LB"), ("Lesotho", "LS"),
    ("Liberia", "LR"), ("Libya", "LY"), ("Liechtenstein", "LI"),
    ("Lithuania", "LT"), ("Luxembourg", "LU"), ("Macao", "MO"),
    ("Madagascar", "MG"), ("Malawi", "MW"), ("Malaysia", "MY"),
    ("Maldives", "MV"), ("Mali", "ML"), ("Malta", "MT"),
    ("Marshall Islands", "MH"), ("Martinique", "MQ"), ("Mauritania", "MR"),
    ("Mauritius", "MU"), ("Mayotte", "YT"), ("Mexico", "MX"),
    ("Micronesia", "FM"), ("Moldova", "MD"), ("Monaco", "MC"),
    ("Mongolia", "MN"), ("Montenegro", "ME"), ("Montserrat", "MS"),
    ("Morocco", "MA"), ("Mozambique", "MZ"), ("Myanmar", "MM"),
    ("Namibia", "NA"), ("Nauru", "NR"), ("Nepal", "NP"),
    ("Netherlands", "NL"), ("New Zealand", "NZ"), ("Nicaragua", "NI"),
    ("Niger", "NE"), ("Nigeria", "NG"), ("Niue", "NU"),
    ("Norway", "NO"), ("Oman", "OM"), ("Pakistan", "PK"),
    ("Palau", "PW"), ("Panama", "PA"), ("Papua New Guinea", "PG"),
    ("Paraguay", "PY"), ("Peru", "PE"), ("Philippines", "PH"),
    ("Poland", "PL"), ("Portugal", "PT"), ("Puerto Rico", "PR"),
    ("Qatar", "QA"), ("Romania", "RO"), ("Russia", "RU"),
    ("Rwanda", "RW"), ("Saint Lucia", "LC"),
    ("Samoa", "WS"), ("San Marino", "SM"),
    ("Saudi Arabia", "SA"), ("Senegal", "SN"), ("Serbia", "RS"),
    ("Seychelles", "SC"), ("Sierra Leone", "SL"), ("Singapore", "SG"),
    ("Slovakia", "SK"), ("Slovenia", "SI"), ("Somalia", "SO"),
    ("South Africa", "ZA"), ("Spain", "ES"), ("Sri Lanka", "LK"),
    ("Sudan", "SD"), ("Suriname", "SR"),
    ("Sweden", "SE"), ("Switzerland", "CH"), ("Syria", "SY"),
    ("Taiwan", "TW"), ("Tajikistan", "TJ"), ("Tanzania", "TZ"),
    ("Thailand", "TH"), ("Togo", "TG"), ("Tonga", "TO"),
    ("Trinidad and Tobago", "TT"), ("Tunisia", "TN"), ("Turkey", "TR"),
    ("Turkmenistan", "TM"), ("Tuvalu", "TV"), ("Uganda", "UG"),
    ("Ukraine", "UA"), ("United Arab Emirates", "AE"),
    ("United Kingdom", "GB"), ("United States", "US"),
    ("Uruguay", "UY"), ("Uzbekistan", "UZ"),
    ("Vanuatu", "VU"), ("Venezuela", "VE"),
    ("Vietnam", "VN"), ("Yemen", "YE"),
    ("Zambia", "ZM"), ("Zimbabwe", "ZW")
]


# ----------------------------------------------------------
# YOUTUBE ENDPOINTS
# ----------------------------------------------------------
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"


# ----------------------------------------------------------
# DATABASE FUNCTIONS
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
        INSERT INTO videos (video_id, keyword, title, description, thumbnail,
                            views, subscribers, published_at, last_seen)
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
        record["video_id"], record["keyword"], record["title"],
        record["description"], record["thumbnail"],
        record["views"], record["subscribers"],
        record["published_at"], record["last_seen"]
    ))
    conn.commit()


def delete_video(conn, video_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM videos WHERE video_id=?", (video_id,))
    conn.commit()


def remove_keyword(conn, video_id, target_kw):
    cur = conn.cursor()
    cur.execute("SELECT keyword FROM videos WHERE video_id=?", (video_id,))
    row = cur.fetchone()
    if not row:
        return
    keywords = [k.strip() for k in row[0].split(",") if k.strip() != target_kw]
    updated = ",".join(keywords)
    cur.execute("UPDATE videos SET keyword=? WHERE video_id=?", (updated, video_id))
    conn.commit()


def fetch_all_saved(conn):
    return pd.read_sql_query("SELECT * FROM videos ORDER BY last_seen DESC", conn)


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------
def short_text(text, limit=200):
    return text if len(text) <= limit else text[:limit] + "..."


def compute_score(views, subs):
    return views / (subs + 1)


def iso_days(days):
    return (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"


# ----------------------------------------------------------
# ASYNC FETCH
# ----------------------------------------------------------
async def fetch_json(session, url, params):
    async with session.get(url, params=params, timeout=15) as resp:
        if resp.status == 200:
            return await resp.json()
    return {}


async def process_keywords(keywords, days, max_results, min_views, max_subs, region_code, progress_cb):
    published_after = iso_days(days)

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Search API calls
        tasks = []
        for kw in keywords:
            params = {
                "part": "snippet", "q": kw,
                "type": "video",
                "order": "viewCount",
                "publishedAfter": published_after,
                "maxResults": max_results,
                "key": YOUTUBE_API_KEY
            }
            if region_code:
                params["regionCode"] = region_code
            tasks.append(fetch_json(session, YOUTUBE_SEARCH_URL, params))

        responses = await asyncio.gather(*tasks)

        videos = {}
        for i, res in enumerate(responses):
            kw = keywords[i]
            for item in res.get("items", []):
                vid = item["id"]["videoId"]
                videos[vid] = {"keyword": kw, "snippet": item["snippet"]}

        vid_list = list(videos.keys())
        final = []

        # process in batches
        for i in range(0, len(vid_list), 40):
            batch = vid_list[i:i+40]

            # video stats
            data_v = await fetch_json(session, YOUTUBE_VIDEO_URL, {
                "part": "statistics", "id": ",".join(batch), "key": YOUTUBE_API_KEY
            })
            vstats = {v["id"]: v["statistics"] for v in data_v.get("items", [])}

            # channel stats
            chan_ids = [videos[v]["snippet"]["channelId"] for v in batch]
            data_c = await fetch_json(session, YOUTUBE_CHANNEL_URL, {
                "part": "statistics", "id": ",".join(chan_ids), "key": YOUTUBE_API_KEY
            })
            cstats = {c["id"]: c["statistics"] for c in data_c.get("items", [])}

            # build final records
            for vid in batch:
                sn = videos[vid]["snippet"]
                views = int(vstats.get(vid, {}).get("viewCount", 0))
                subs = int(cstats.get(sn["channelId"], {}).get("subscriberCount", 0))

                if views >= min_views and subs <= max_subs:
                    final.append({
                        "video_id": vid,
                        "keyword": videos[vid]["keyword"],
                        "title": sn.get("title", ""),
                        "description": short_text(sn.get("description", "")),
                        "thumbnail": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
                        "views": views,
                        "subscribers": subs,
                        "published_at": sn.get("publishedAt"),
                        "last_seen": datetime.utcnow().strftime("%Y-%m-%d")
                    })

            progress_cb(min(i+40, len(vid_list)), len(vid_list))

        return final


# ----------------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------------
st.set_page_config(page_title="YouTube Viral Finder", layout="wide")
conn = init_db()

st.title("🔥 YouTube Viral Keyword Finder — Worldwide Region Support 🌍")

# Keep results after rerun
if "results" not in st.session_state:
    st.session_state["results"] = []


# ------------------ SIDEBAR ------------------
with st.sidebar:
    st.header("Search Filters")

    days = st.slider("Days to search", 1, 30, 5)
    max_results = st.number_input("Max results per keyword", 1, 50, 10)
    min_views = st.number_input("Minimum Views", 0, 10000000, 100)
    max_subs = st.number_input("Max Channel Subscribers", 0, 50000000, 5000)

    # Full country list
    selected_country = st.selectbox(
        "Select Country",
        COUNTRIES,
        format_func=lambda x: x[0]
    )
    region_code = selected_country[1]

    keywords_text = st.text_area("Keywords", "Reddit Cheating\nAITA\nStory Time")
    keywords = [k.strip() for k in keywords_text.split("\n") if k.strip()]

    st.write("---")

    if st.button("Download Saved CSV"):
        df_all = fetch_all_saved(conn)
        st.download_button("Download File", df_all.to_csv(index=False), "saved_videos.csv")

    if st.button("Clear Saved Database"):
        cur = conn.cursor()
        cur.execute("DELETE FROM videos")
        conn.commit()
        st.success("Database cleared.")


# ------------------ MAIN UI ------------------
col1, col2 = st.columns([2, 1])


# LEFT SIDE – SEARCH RESULTS
with col1:
    st.subheader("Live YouTube Search")

    if st.button("Fetch Data"):
        if not keywords:
            st.warning("Please enter at least one keyword.")
        else:
            progress = st.progress(0)
            txt = st.empty()

            def update(done, total):
                progress.progress(int(done/total*100))
                txt.text(f"Processed {done}/{total}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                process_keywords(
                    keywords, days, max_results,
                    min_views, max_subs,
                    region_code,
                    update
                )
            )

            st.session_state["results"] = results
            st.success(f"Found {len(results)} videos!")

    # Display saved session results
    if st.session_state["results"]:
        df = pd.DataFrame(st.session_state["results"])
        df["viral_score"] = df.apply(lambda r: compute_score(r["views"], r["subscribers"]), axis=1)
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

                if st.button(f"Save {row['video_id']}", key=f"save_{row['video_id']}"):
                    upsert_video(conn, row.to_dict())
                    st.success("Saved!")


# RIGHT SIDE – SAVED DATA
with col2:
    st.subheader("Saved Videos (Database)")

    df_saved = fetch_all_saved(conn)
    st.write(f"Total saved: **{len(df_saved)}**")

    if len(df_saved):

        df_saved["viral_score"] = df_saved.apply(
            lambda r: compute_score(int(r["views"]), int(r["subscribers"])), axis=1
        )

        st.markdown("### Top Keywords")
        explode_df = df_saved.assign(kw=df_saved["keyword"].str.split(",")).explode("kw")
        st.table(explode_df["kw"].value_counts().head(10))

        st.markdown("### Saved Videos")

        for _, row in df_saved.iterrows():
            vid = row["video_id"]
            keywords_list = [k.strip() for k in row["keyword"].split(",") if k.strip()]

            with st.expander(f"{row['title']}"):
                c1, c2 = st.columns([1, 3])

                with c1:
                    if row["thumbnail"]:
                        st.image(row["thumbnail"], width=120)

                with c2:
                    st.write(f"Views: {row['views']} — Subs: {row['subscribers']} — Score: {row['viral_score']:.2f}")
                    st.write(short_text(row["description"]))

                    # Remove keyword
                    st.write("**Keywords:**")
                    for kw in keywords_list:
                        if st.button(f"Remove '{kw}'", key=f"rm_{vid}_{kw}"):
                            remove_keyword(conn, vid, kw)
                            st.success("Keyword removed.")
                            st.experimental_rerun()

                    # Delete video
                    if st.button(f"Delete Video", key=f"del_{vid}"):
                        delete_video(conn, vid)
                        st.success("Deleted.")
                        st.experimental_rerun()

        # Chart
        top10 = df_saved.sort_values("viral_score", ascending=False).head(10)
        fig = plt.figure(figsize=(5, 3))
        plt.bar(range(len(top10)), top10["viral_score"])
        plt.xticks(range(len(top10)), top10["title"].str[:20], rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

