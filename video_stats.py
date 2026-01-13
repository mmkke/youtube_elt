import os
import requests
import json

from dotenv import load_dotenv

load_dotenv()
DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = "hoer.berlin"
PART = "contentDetails"
URL = f'https://youtube.googleapis.com/youtube/v3/channels?part={PART}&forHandle={CHANNEL_HANDLE}&key={DEVELOPER_KEY}'

def get_playlist_id(url: str, save_json: bool = False):
    """
    Fetch YouTube channel metadata and return the uploads playlist ID.

    Returns:
        channel_items (dict)
        uploads_playlist_id (str)
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if save_json:
            with open("response.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        items = data.get("items", [])
        if not items:
            raise ValueError("No channel items returned")

        channel_items = items[0]

        uploads_playlist_id = (
            channel_items
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )

        if not uploads_playlist_id:
            raise ValueError("Uploads playlist ID not found")

        return uploads_playlist_id, channel_items

    except requests.exceptions.RequestException as e:
        raise RuntimeError("HTTP error while fetching channel data") from e
    except (KeyError, ValueError) as e:
        raise RuntimeError("Unexpected YouTube API response structure") from e
    
    
if __name__ == "__main__":
    playlistID, _ = get_playlist_id(URL)
    print(playlistID)