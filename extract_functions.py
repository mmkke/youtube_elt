import os
import requests
import json
from datetime import date
from pathlib import Path
from pprint import pprint
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = "RJTheBikeGuy" #"hoer.berlin"
JSON_PATH = f"data/{CHANNEL_HANDLE}"

def get_playlist_id(channel_handle: str, api_key: str, save_json: bool = False):
    """
    Retrieve the uploads playlist ID for a YouTube channel.

    This function queries the YouTube Data API v3 `channels.list`
    endpoint using a channel handle (via the `forHandle` parameter)
    and extracts the channel's uploads playlist ID from the response.

    Parameters
    ----------
    channel_handle : str
        YouTube channel handle (with or without leading '@').
    api_key : str
        YouTube Data API v3 developer key.
    save_json : bool, optional
        If True, save the raw API response to `response.json`
        for debugging or inspection. Default is False.

    Returns
    -------
    tuple
        uploads_playlist_id : str
            Playlist ID containing all uploaded videos for the channel.
        channel_items : dict
            Full channel metadata object returned by the API.

    Raises
    ------
    RuntimeError
        If an HTTP error occurs while querying the YouTube API.
    RuntimeError
        If the API response structure is unexpected or missing
        required fields.
    """

    url = "https://youtube.googleapis.com/youtube/v3/channels"

    # Normalize handle to allow both '@handle' and 'handle'
    handle = channel_handle.lstrip("@")

    params = {
        "part": "contentDetails",
        "forHandle": handle,
        "key": api_key
    }

    try:
        # Execute API request
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Optionally persist raw response for debugging
        if save_json:
            with open("response.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        # Validate response contains channel items
        items = data.get("items", [])
        if not items:
            raise ValueError("No channel items returned")

        channel_items = items[0]

        # Safely extract uploads playlist ID
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
    


def get_video_ids(playlist_id: str, api_key: str, max_results: int = 50):
    """
    Retrieve all video IDs from a YouTube playlist.

    This function queries the YouTube Data API v3 `playlistItems.list`
    endpoint and paginates through all available pages to collect
    every video ID contained in the specified playlist.

    Parameters
    ----------
    playlist_id : str
        The YouTube playlist ID (e.g., a channel's uploads playlist).
    api_key : str
        YouTube Data API v3 developer key.
    max_results : int, optional
        Maximum number of playlist items returned per request.
        Must be between 1 and 50 (inclusive). Default is 50.

    Returns
    -------
    list[str]
        A list of YouTube video IDs in the order returned by the API.

    Raises
    ------
    ValueError
        If `max_results` is not between 1 and 50.
    RuntimeError
        If an HTTP error occurs while querying the YouTube API.
    """

    # Enforce YouTube API constraints for pagination size
    if max_results <= 0 or max_results > 50:
        raise ValueError("max_results must be between 1 and 50 (inclusive).")

    video_ids = []
    base_url = "https://youtube.googleapis.com/youtube/v3/playlistItems"

    # Base query parameters; pageToken is added dynamically during pagination
    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": max_results,
        "key": api_key,
    }

    try:
        # Paginate until no nextPageToken is returned
        while True:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract video IDs from each playlist item
            for item in data.get("items", []):
                video_id = (
                    item
                    .get("contentDetails", {})
                    .get("videoId")
                )
                if video_id:
                    video_ids.append(video_id)

            # Stop when there are no more pages
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            # Request the next page on the next iteration
            params["pageToken"] = next_page_token

        return video_ids

    except requests.exceptions.RequestException as e:
        raise RuntimeError("HTTP error while fetching playlist items") from e


def extract_video_detail(video_ids, api_key: str, batch_size: int = 50):
    """
    Fetch detailed metadata for a list of YouTube video IDs.

    This function batches requests to the YouTube Data API v3
    `videos.list` endpoint (maximum 50 video IDs per request)
    and aggregates video-level metadata across all batches.

    Parameters
    ----------
    video_ids : list[str]
        List of YouTube video IDs to fetch metadata for.
    api_key : str
        YouTube Data API v3 developer key.
    batch_size : int, optional
        Number of video IDs per request batch. Must be between
        1 and 50 (inclusive). Default is 50.

    Returns
    -------
    list[dict]
        A list of dictionaries, one per video, containing:
            - video_id
            - title
            - publishedAt
            - duration (ISO 8601 format, e.g. 'PT12M34S')
            - viewCount
            - likeCount
            - favoriteCount
            - commentCount

    Raises
    ------
    ValueError
        If `batch_size` is not between 1 and 50.
    RuntimeError
        If an HTTP error occurs while querying the YouTube API.
    """

    # Enforce YouTube API batch size constraints
    if batch_size <= 0 or batch_size > 50:
        raise ValueError("batch_size must between 1 and 50 (inclusive).")

    def batch_list(items, n):
        """
        Yield successive n-sized batches from a list.

        Parameters
        ----------
        items : list
            Input list to batch.
        n : int
            Batch size.

        Yields
        ------
        list
            Sub-list of length n (or smaller for final batch).
        """
        for i in range(0, len(items), n):
            yield items[i:i + n]

    extracted = []
    url = "https://youtube.googleapis.com/youtube/v3/videos"

    try:
        # Iterate through video IDs in API-sized batches
        for batch in batch_list(video_ids, batch_size):
            params = {
                "part": "contentDetails,snippet,statistics",
                "id": ",".join(batch),  # comma-separated list of IDs
                "key": api_key,
            }

            # Execute API request
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            # Parse each returned video item
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
    

def save_video_data_to_json(extracted_data, path):
    
    path = Path(path)
    json_path = path.with_name(f"{path.stem}_{date.today()}.json")
    try:
        with open(json_path, "w", encoding="utf-8") as json_outfile:
            json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
        print(f"Data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving data to {json_path}: {e}")
    


if __name__ == "__main__":
    playlistID, _ = get_playlist_id(
                                    channel_handle=CHANNEL_HANDLE, 
                                    api_key=API_KEY, 
                                    save_json=True
                                    )
    print("=="*25)   
    print(f"Playlist ID: {playlistID}")
    print()

    video_ids = get_video_ids(
                            playlistID, 
                            api_key=API_KEY, 
                            max_results=50
                            )
    print("=="*25)
    print(f"Length of ID list: {len(video_ids)}")
    print()

    extracted_data = extract_video_detail(
                                        video_ids=video_ids,
                                        api_key=API_KEY,
                                        batch_size=50
                                        )
    print("=="*25)  
    print(f"Number of results: {len(extracted_data)}")
    print()

    # Check that an entry was returned for all ids in list.
    assert len(video_ids) == len(extracted_data), (
                f"The length of the extracted data ({len(extracted_data)})" 
                f"does not match the length of the video_ids list ({len(video_ids)})"
                )
    
    # Review the first n results
    n=10
    print("=="*25)
    print(f"First {n} entries in extracted data.")
    for data in extracted_data[:n]:
        print("--"*25)
        pprint(data)
        print()

    save_video_data_to_json(
                            extracted_data=extracted_data,
                            path = JSON_PATH)
    
    print("Done.")