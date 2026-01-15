import requests
import json
from datetime import date
from pathlib import Path
from pprint import pprint

from airflow.decorators import task
from airflow.models import Variable


def _get_api_key() -> str:
    """
    Fetch the YouTube API key at *task runtime*.

    Airflow best practice: avoid Variable.get() at module import time.
    """
    api_key = Variable.get("API_KEY", default_var=None)
    if not api_key:
        raise RuntimeError(
            "Missing Airflow Variable 'API_KEY'. Set it in Airflow UI or via env/CLI."
        )
    return api_key


@task
def get_playlist_id(channel_handle: str, save_json: bool = False) -> str:
    """
    Retrieve the uploads playlist ID for a YouTube channel.

    Parameters
    ----------
    channel_handle : str
        YouTube channel handle (with or without leading '@').
    save_json : bool, optional
        If True, save raw API response to `response.json`.

    Returns
    -------
    str
        uploads_playlist_id : Playlist ID containing all uploaded videos.

    Raises
    ------
    RuntimeError
        If an HTTP error occurs or response structure is unexpected.
    """
    api_key = _get_api_key()
    url = "https://youtube.googleapis.com/youtube/v3/channels"

    # Normalize handle to allow both '@handle' and 'handle'
    handle = channel_handle.lstrip("@")

    params = {
        "part": "contentDetails",
        "forHandle": handle,
        "key": api_key,
    }

    try:
        response = requests.get(url, params=params)
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

        return uploads_playlist_id

    except requests.exceptions.RequestException as e:
        raise RuntimeError("HTTP error while fetching channel data") from e
    except (KeyError, ValueError) as e:
        raise RuntimeError("Unexpected YouTube API response structure") from e


@task
def get_video_ids(playlist_id: str, max_results: int = 50) -> list[str]:
    """
    Retrieve all video IDs from a YouTube playlist (paginated).

    Parameters
    ----------
    playlist_id : str
        YouTube playlist ID.
    max_results : int, optional
        Max results per page (1–50). Default 50.

    Returns
    -------
    list[str]
        Video IDs contained in the playlist.
    """
    api_key = _get_api_key()

    if max_results <= 0 or max_results > 50:
        raise ValueError("max_results must be between 1 and 50 (inclusive).")

    video_ids: list[str] = []
    url = "https://youtube.googleapis.com/youtube/v3/playlistItems"

    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": max_results,
        "key": api_key,
    }

    try:
        while True:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item.get("contentDetails", {}).get("videoId")
                if video_id:
                    video_ids.append(video_id)

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            params["pageToken"] = next_page_token

        return video_ids

    except requests.exceptions.RequestException as e:
        raise RuntimeError("HTTP error while fetching playlist items") from e


@task
def extract_video_detail(video_ids: list[str], batch_size: int = 50) -> list[dict]:
    """
    Fetch detailed metadata for a list of YouTube video IDs (batched, max 50 IDs per request).

    Parameters
    ----------
    video_ids : list[str]
        Video IDs to fetch details for.
    batch_size : int, optional
        Batch size (1–50). Default 50.

    Returns
    -------
    list[dict]
        One dict per video with snippet/contentDetails/statistics fields.
    """
    api_key = _get_api_key()

    if batch_size <= 0 or batch_size > 50:
        raise ValueError("batch_size must between 1 and 50 (inclusive).")

    def batch_list(items: list[str], n: int):
        for i in range(0, len(items), n):
            yield items[i:i + n]

    extracted: list[dict] = []
    url = "https://youtube.googleapis.com/youtube/v3/videos"

    try:
        for batch in batch_list(video_ids, batch_size):
            params = {
                "part": "contentDetails,snippet,statistics",
                "id": ",".join(batch),
                "key": api_key,
            }

            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("items", []):
                snippet = item.get("snippet", {}) or {}
                stats = item.get("statistics", {}) or {}
                details = item.get("contentDetails", {}) or {}

                extracted.append({
                    "video_id": item.get("id"),
                    "title": snippet.get("title"),
                    "publishedAt": snippet.get("publishedAt"),
                    "duration": details.get("duration"),
                    "viewCount": stats.get("viewCount"),
                    "likeCount": stats.get("likeCount"),
                    "favoriteCount": stats.get("favoriteCount"),
                    "commentCount": stats.get("commentCount"),
                })

        return extracted

    except requests.exceptions.RequestException as e:
        raise RuntimeError("HTTP error while fetching video details") from e


@task
def save_video_data_to_json(extracted_data: list[dict], path: str) -> str:
    """Save extracted data to a date-stamped JSON file and return the path."""
    path_obj = Path(path)
    json_path = path_obj.with_name(f"{path_obj.stem}_{date.today()}.json")
    json_path.parent.mkdir(parents=True, exist_ok=True)

    with open(json_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

    print(f"Data saved to: {json_path}")
    return str(json_path)


if __name__ == "__main__":
    # Optional local runner (not typical for Airflow task modules).
    # Requires AIRFLOW to be configured because Variable.get() is used.
    channel_handle = Variable.get("CHANNEL_HANDLE")
    json_path = f"data/{channel_handle}"

    playlist_id = get_playlist_id(channel_handle=channel_handle, save_json=True)
    print(f"Playlist ID: {playlist_id}")

    video_ids = get_video_ids(playlist_id=playlist_id, max_results=50)
    print(f"Num videos: {len(video_ids)}")

    extracted_data = extract_video_detail(video_ids=video_ids, batch_size=50)
    print(f"Num extracted: {len(extracted_data)}")

    assert len(video_ids) == len(extracted_data), (
        f"Extracted ({len(extracted_data)}) != video_ids ({len(video_ids)})"
    )

    for data in extracted_data[:10]:
        pprint(data)

    save_video_data_to_json(extracted_data=extracted_data, path=json_path)
    print("Done.")