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
# WORLD COUNTRIES (NAME + CODE)
# ----------------------------------------------------------
COUNTRIES = [
    ("Select Country (Global Search)", ""),
    ("Afghanistan", "AF"), ("Albania", "AL"), ("Algeria", "DZ"), ("American Samoa", "AS"),
    ("Andorra", "AD"), ("Angola", "AO"), ("Anguilla", "AI"), ("Antarctica", "AQ"),
    ("Antigua and Barbuda", "AG"), ("Argentina", "AR"), ("Armenia", "AM"), ("Aruba", "AW"),
    ("Australia", "AU"), ("Austria", "AT"), ("Azerbaijan", "AZ"), ("Bahamas", "BS"),
    ("Bahrain", "BH"), ("Bangladesh", "BD"), ("Barbados", "BB"), ("Belarus", "BY"),
    ("Belgium", "BE"), ("Belize", "BZ"), ("Benin", "BJ"), ("Bermuda", "BM"),
    ("Bhutan", "BT"), ("Bolivia", "BO"), ("Bosnia and Herzegovina", "BA"),
    ("Botswana", "BW"), ("Brazil", "BR"), ("Brunei", "BN"), ("Bulgaria", "BG"),
    ("Burkina Faso", "BF"), ("Burundi", "BI"), ("Cambodia", "KH"), ("Cameroon", "CM"),
    ("Canada", "CA"), ("Cape Verde", "CV"), ("Cayman Islands", "KY"), ("Chad", "TD"),
    ("Chile", "CL"), ("China", "CN"), ("Colombia", "CO"), ("Comoros", "KM"),
    ("Congo", "CG"), ("Costa Rica", "CR"), ("Croatia", "HR"), ("Cuba", "CU"),
    ("Cyprus", "CY"), ("Czech Republic", "CZ"), ("Denmark", "DK"), ("Djibouti", "DJ"),
    ("Dominica", "DM"), ("Dominican Republic", "DO"), ("Ecuador", "EC"), ("Egypt", "EG"),
    ("El Salvador", "SV"), ("Equatorial Guinea", "GQ"), ("Eritrea", "ER"), ("Estonia", "EE"),
    ("Ethiopia", "ET"), ("Fiji", "FJ"), ("Finland", "FI"), ("France", "FR"),
    ("Gabon", "GA"), ("Gambia", "GM"), ("Georgia", "GE"), ("Germany", "DE"),
    ("Ghana", "GH"), ("Greece", "GR"), ("Greenland", "GL"), ("Grenada", "GD"),
    ("Guatemala", "GT"), ("Guinea", "GN"), ("Guyana", "GY"), ("Haiti", "HT"),
    ("Honduras", "HN"), ("Hong Kong", "HK"), ("Hungary", "HU"), ("Iceland", "IS"),
    ("India", "IN"), ("Indonesia", "ID"), ("Iran", "IR"), ("Iraq", "IQ"),
    ("Ireland", "IE"), ("Isle of Man", "IM"), ("Israel", "IL"), ("Italy", "IT"),
    ("Jamaica", "JM"), ("Japan", "JP"), ("Jordan", "JO"), ("Kazakhstan", "KZ"),
    ("Kenya", "KE"), ("Kiribati", "KI"), ("South Korea", "KR"), ("Kuwait", "KW"),
    ("Kyrgyzstan", "KG"), ("Laos", "LA"), ("Latvia", "LV"), ("Lebanon", "LB"),
    ("Lesotho", "LS"), ("Liberia", "LR"), ("Libya", "LY"), ("Liechtenstein", "LI"),
    ("Lithuania", "LT"), ("Luxembourg", "LU"), ("Macao", "MO"), ("Madagascar", "MG"),
    ("Malawi", "MW"), ("Malaysia", "MY"), ("Maldives", "MV"), ("Mali", "ML"),
    ("Malta", "MT"), ("Marshall Islands", "MH"), ("Martinique", "MQ"),
    ("Mauritania", "MR"), ("Mauritius", "MU"), ("Mayotte", "YT"), ("Mexico", "MX"),
    ("Micronesia", "FM"), ("Moldova", "MD"), ("Monaco", "MC"), ("Mongolia", "MN"),
    ("Montenegro", "ME"), ("Montserrat", "MS"), ("Morocco", "MA"), ("Mozambique", "MZ"),
    ("Myanmar", "MM"), ("Namibia", "NA"), ("Nauru", "NR"), ("Nepal", "NP"),
    ("Netherlands", "NL"), ("New Zealand", "NZ"), ("Nicaragua", "NI"), ("Niger", "NE"),
    ("Nigeria", "NG"), ("Niue", "NU"), ("Norway", "NO"), ("Oman", "OM"),
    ("Pakistan", "PK"), ("Palau", "PW"), ("Panama", "PA"), ("Papua New Guinea", "PG"),
    ("Paraguay", "PY"), ("Peru", "PE"), ("Philippines", "PH"), ("Poland", "PL"),
    ("Portugal", "PT"), ("Puerto Rico", "PR"), ("Qatar", "QA"), ("Romania", "RO"),
    ("Russia", "RU"), ("Rwanda", "RW"), ("Saint Helena", "SH"), ("Saint Kitts", "KN"),
    ("Saint Lucia", "LC"), ("Samoa", "WS"), ("San Marino", "SM"),
    ("Saudi Arabia", "SA"), ("Senegal", "SN"), ("Serbia", "RS"), ("Seychelles", "SC"),
    ("Sierra Leone", "SL"), ("Singapore", "SG"), ("Slovakia", "SK"),
    ("Slovenia", "SI"), ("Somalia", "SO"), ("South Africa", "ZA"), ("Spain", "ES"),
    ("Sri Lanka", "LK"), ("Sudan", "SD"), ("Suriname", "SR"), ("Sweden", "SE"),
    ("Switzerland", "CH"), ("Syria", "SY"), ("Taiwan", "TW"), ("Tajikistan", "TJ"),
    ("Tanzania", "TZ"), ("Thailand", "TH"), ("Togo", "TG"), ("Tonga", "TO"),
    ("Trinidad and Tobago", "TT"), ("Tunisia", "TN"), ("Turkey", "TR"),
    ("Turkmenistan", "TM"), ("Tuvalu", "TV"), ("Uganda", "UG"), ("Ukraine", "UA"),
    ("United Arab Emirates", "AE"), ("United Kingdom", "GB"), ("United States", "US"),
    ("Uruguay", "UY"), ("Uzbekistan", "UZ"), ("Vanuatu", "VU"), ("Venezuela", "VE"),
    ("Vietnam", "VN"), ("Yemen", "YE"), ("Zambia", "ZM"), ("Zimbabwe", "ZW")
]

# ----------------------------------------------------------
# DATABASE
# ----------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_FILENAME, check_same_thread=False)
    conn.execute("""
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
    return conn


def upsert_video(conn, r):
    conn.execute("""
        INSERT INTO videos (video_id, keyword, title, description, thumbnail,
                            views, subscribers, published_at, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            views=excluded.views,
            subscribers=excluded.subscribers,
            last_seen=excluded.last_seen,
            description=excluded.description,
            keyword=CASE
                WHEN instr(videos.keyword, excluded.keyword)=0
                THEN videos.keyword || ',' || excluded.keyword
                ELSE videos.keyword END
    """, (
        r["video_id"], r["keyword"], r["title"], r["description"],
        r["thumbnail"], r["views"], r["subscribers"],
        r["published_at"], r["last_seen"]
    ))
    conn.commit()


def delete_video(conn, vid):
    conn.execute("DELETE FROM videos WHERE video_id=?", (vid,))
    conn.commit()


def remove_keyword(conn, vid, kw):
    cur = conn.cursor()
    cur.execute("SELECT keyword FROM videos WHERE video_id=?", (vid,))
    row = cur.fetchone()
    if not row:
        return
    updated = ",".join([k for k in row[0].split(",") if k.strip() != kw])
    conn.execute("UPDATE videos SET keyword=? WHERE video_id=?", (updated, vid))
    conn.commit()


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------
def short(text):
    return text[:200] + "..." if len(text) > 200 else text


def score(views, subs):
    return views / (subs + 1)


def iso_days(days):
    return (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"


# ----------------------------------------------------------
# ASYNC FETCH
# ----------------------------------------------------------
async def fetch_json(session, url, params):
    async with session.get(url, params=params, timeout=20) as resp:
        if resp.status == 200:
            return await resp.json()
    return {}


async def process_keywords(keywords, days, max_results, min_views, max_subs, region_code, cb):
    published_after = iso_days(days)

    async with aiohttp.ClientSession() as session:

        tasks = []
        for kw in keywords:
            p = {
                "part": "snippet", "q": kw, "type": "video", "order": "viewCount",
                "publishedAfter": published_after, "maxResults": max_results,
                "key": YOUTUBE_API_KEY
            }
            if region_code:
                p["regionCode"] = region_code
            tasks.append(fetch_json(session, "https://www.googleapis.com/youtube/v3/search", p))

        responses = await asyncio.gather(*tasks)

        videos = {}
        for i, res in enumerate(responses):
            for item in res.get("items", []):
                vid = item["id"]["videoId"]
                videos[vid] = {"kw": keywords[i], "snip": item["snippet"]}

        final = []
        ids = list(videos.keys())

        for i in range(0, len(ids), 40):
            batch = ids[i:i+40]
            cb(min(i+40, len(ids)), len(ids))

            # video stats
            vs = await fetch_json(session, "https://www.googleapis.com/youtube/v3/videos", {
                "part": "statistics", "id": ",".join(batch), "key": YOUTUBE_API_KEY
            })
            vstats = {v["id"]: v["statistics"] for v in vs.get("items", [])}

            # channel stats
            chan_ids = [videos[v]["snip"]["channelId"] for v in batch]
            cs = await fetch_json(session, "https://www.googleapis.com/youtube/v3/channels", {
                "part": "statistics", "id": ",".join(chan_ids), "key": YOUTUBE_API_KEY
            })
            cstats = {c["id"]: c["statistics"] for c in cs.get("items", [])}

            for vid in batch:
                sn = videos[vid]["snip"]
                views = int(vstats.get(vid, {}).get("viewCount", 0))
                subs = int(cstats.get(sn["channelId"], {}).get("subscriberCount", 0))

                if views >= min_views and subs <= max_subs:
                    final.append({
                        "video_id": vid,
                        "keyword": videos[vid]["kw"],
                        "title": sn.get("title", ""),
                        "description": short(sn.get("description", "")),
                        "thumbnail": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
                        "views": views,
                        "subscribers": subs,
                        "published_at": sn.get("publishedAt"),
                        "last_seen": datetime.utcnow().strftime("%Y-%m-%d")
                    })

        return final


# ----------------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------------
st.set_page_config(page_title="YouTube Viral Finder", layout="wide")
conn = init_db()

st.title("🔥 YouTube Viral Keyword Finder — Worldwide 🌍")

if "results" not in st.session_state:
    st.session_state["results"] = []

# SIDEBAR
with st.sidebar:
    st.header("Filters")

    days = st.slider("Days", 1, 30, 5)
    max_results = st.number_input("Max Results", 1, 50, 10)
    min_views = st.number_input("Minimum Views", 0, 10000000, 100)
    max_subs = st.number_input("Maximum Subscribers", 0, 50000000, 5000)

    selected_country = st.selectbox("Select Country", COUNTRIES, format_func=lambda x: x[0])
    region_code = selected_country[1]

    keywords_text = st.text_area("Keywords", "Reddit Cheating\nAITA\nStory")
    keywords = [k.strip() for k in keywords_text.split("\n") if k.strip()]

    st.write("---")

    if st.button("Download Saved CSV"):
        df_all = pd.read_sql("SELECT * FROM videos", conn)
        st.download_button("Download File", df_all.to_csv(index=False), "saved_videos.csv")

    if st.button("Clear Database"):
        conn.execute("DELETE FROM videos")
        st.success("Database cleared.")


col1, col2 = st.columns([2, 1])

# -------- LEFT SIDE ------------
with col1:
    st.subheader("Live Search Results")

    if st.button("Fetch Data"):
        progress = st.progress(0)
        txt = st.empty()

        def update(d, t):
            progress.progress(int(d/t*100))
            txt.text(f"Processed {d}/{t}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            process_keywords(
                keywords, days, max_results,
                min_views, max_subs, region_code, update
            )
        )

        st.session_state["results"] = results
        st.success(f"Found {len(results)} videos!")

    # ⭐ DOWNLOAD LIVE RESULTS CSV
    if st.session_state["results"]:
        df_live = pd.DataFrame(st.session_state["results"])
        df_live["viral_score"] = df_live.apply(lambda r: score(r["views"], r["subscribers"]), axis=1)

        st.download_button(
            "⬇️ Download Fetched Data CSV",
            df_live.to_csv(index=False),
            "fetched_live_results.csv"
        )

        # Show results
        df = df_live.sort_values("viral_score", ascending=False)
        for _, row in df.iterrows():
            c1, c2 = st.columns([1, 3])
            with c1:
                if row["thumbnail"]:
                    st.image(row["thumbnail"], width=150)

            with c2:
                st.markdown(f"### {row['title']}")
                st.write(f"Keyword: `{row['keyword']}`")
                st.write(f"Views: **{row['views']}**, Subs: **{row['subscribers']}**, Score: **{row['viral_score']:.2f}**")
                st.write(row["description"])

                if st.button(f"Save {row['video_id']}", key=f"s_{row['video_id']}"):
                    upsert_video(conn, row.to_dict())
                    st.success("Saved!")


# -------- RIGHT SIDE ------------
with col2:
    st.subheader("Saved Videos")

    df_saved = pd.read_sql("SELECT * FROM videos", conn)
    st.write(f"Total saved: {len(df_saved)}")

    if len(df_saved):

        for _, row in df_saved.iterrows():
            vid = row["video_id"]
            kw_list = [k.strip() for k in row["keyword"].split(",") if k.strip()]

            with st.expander(row["title"]):
                st.write(f"Views: {row['views']} — Subs: {row['subscribers']}")
                st.write(row["description"])

                st.write("Keywords:")
                for kw in kw_list:
                    if st.button(f"Remove {kw}", key=f"rm_{vid}_{kw}"):
                        remove_keyword(conn, vid, kw)
                        st.success("Removed")
                        st.experimental_rerun()

                if st.button("Delete Video", key=f"del_{vid}"):
                    delete_video(conn, vid)
                    st.success("Deleted")
                    st.experimental_rerun()
