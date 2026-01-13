# -*- coding: utf-8 -*-

# Sample Python code for youtube.videos.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python

import os
import googleapiclient.discovery

from dotenv import load_dotenv

load_dotenv()
DEVELOPER_KEY = os.getenv("YOUTUBE_API_KEY")

def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "YOUR_API_KEY"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    request = youtube.videos().list(
        part="statistics",
        id="KHXYN1JH8Oc"
    )
    response = request.execute()

    print(response)

if __name__ == "__main__":
    main()