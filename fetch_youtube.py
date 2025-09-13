import os
import pandas as pd
import requests
from datetime import datetime

# --------------------------------
# Config
# --------------------------------
API_KEY = os.getenv("YOUTUBE_API_KEY")  # use env var for security
SEARCH_QUERY = "Apple"
OUTPUT_FILE = "apple_youtube.csv"
BACKUP_DIR = "backups"

os.makedirs(BACKUP_DIR, exist_ok=True)


# --------------------------------
# API helpers
# --------------------------------
def youtube_search(query, page_token=None, published_after=None):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": 50,
        "order": "date",
        "key": API_KEY,
    }
    if page_token:
        params["pageToken"] = page_token
    if published_after:
        params["publishedAfter"] = published_after
    return requests.get(url, params=params).json()


def youtube_video_details(video_ids):
    """Fetch snippet + statistics for up to 50 videos at once"""
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "id": ",".join(video_ids),
        "key": API_KEY,
    }
    return requests.get(url, params=params).json()


# --------------------------------
# Load existing CSV
# --------------------------------
if os.path.exists(OUTPUT_FILE):
    df_existing = pd.read_csv(OUTPUT_FILE)
    df_existing["publishedAt"] = pd.to_datetime(df_existing["publishedAt"])
    existing_ids = set(df_existing["videoId"].tolist())

    # Backup old file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"apple_youtube_backup_{timestamp}.csv")
    df_existing.to_csv(backup_file, index=False)
    print(f"Backup saved to: {backup_file}")

    latest_date = df_existing["publishedAt"].max().isoformat() + "Z"
    print(f"Latest saved video date: {latest_date}")
else:
    df_existing = pd.DataFrame()
    existing_ids = set()
    latest_date = "2020-01-01T00:00:00Z"  # starting point


# --------------------------------
# Fetch new videos
# --------------------------------
all_new = []
page_token = None

while True:
    try:
        data = youtube_search(SEARCH_QUERY, page_token, published_after=latest_date)
        if "error" in data:
            print("Error:", data["error"]["message"])
            break

        items = data.get("items", [])
        if not items:
            break

        # Gather new video IDs
        new_ids = [i["id"]["videoId"] for i in items if i["id"]["videoId"] not in existing_ids]

        if new_ids:
            # Get details for those IDs
            details = youtube_video_details(new_ids)
            for vid in details.get("items", []):
                snippet = vid.get("snippet", {})
                stats = vid.get("statistics", {})

                all_new.append({
                    "videoId": vid["id"],
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "publishedAt": snippet.get("publishedAt"),
                    "channelTitle": snippet.get("channelTitle"),
                    "tags": ",".join(snippet.get("tags", [])),
                    "viewCount": stats.get("viewCount"),
                    "likeCount": stats.get("likeCount"),
                    "commentCount": stats.get("commentCount"),
                })

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    except Exception as e:
        print("Stopped due to error:", e)
        break


# --------------------------------
# Save results
# --------------------------------
if all_new:
    df_new = pd.DataFrame(all_new)
    df_new["publishedAt"] = pd.to_datetime(df_new["publishedAt"])

    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset="videoId", inplace=True)

    df_combined.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(df_new)} new videos, total {len(df_combined)}")
else:
    print("ℹ️ No new videos found.")
