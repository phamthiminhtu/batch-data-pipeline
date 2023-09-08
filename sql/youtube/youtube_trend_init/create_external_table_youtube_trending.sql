CREATE OR REPLACE EXTERNAL TABLE {{ params.database_name }}.{{ params.schema_name }}.youtube_trending
    (
        video_id VARCHAR AS (value:c1::VARCHAR),
        title VARCHAR AS (value:c2::VARCHAR),
        publishedAt VARCHAR AS (value:c3::VARCHAR),
        channelId VARCHAR AS (value:c4::VARCHAR),
        channelTitle VARCHAR AS (value:c5::VARCHAR),
        categoryId VARCHAR AS (value:c6::VARCHAR),
        trending_date VARCHAR AS (value:c7::VARCHAR),
        view_count VARCHAR AS (value:c8::VARCHAR),
        likes VARCHAR AS (value:c9::VARCHAR),
        dislikes VARCHAR AS (value:c10::VARCHAR),
        comment_count VARCHAR AS (value:c11::VARCHAR),
        comments_disabled VARCHAR AS (value:c12::VARCHAR)
        comments_disabled VARCHAR AS (value:c12::VARCHAR)
        
    )
FILE_FORMAT = my_csv_format
WITH location = @{{ params.youtube_stage_name }}/youtube-trending
refresh_on_create = TRUE
auto_refresh = TRUE;