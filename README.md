# youtube_elt


### To do list:
    - finish testing
        - unit: Done!
        - integration: Done!
        - end-to-end
    - Refactor code and create package source code: DONE
    - Add timestamps to ingestion pipeline
        - Add ingested_at, seen_last_at, updated_last_at
    - incorporate slowly changing dimensions
        - Add a daily snapshot table for quickly changing metrics like comment_count
            - core.yt_api_metrics_daily

            sql ```
            

            ```
        - Add and SCD2 table for dimensions like video_title, duration, 
            - core.yt_api_dim_history


