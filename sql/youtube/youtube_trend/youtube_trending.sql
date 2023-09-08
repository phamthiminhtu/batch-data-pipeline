INSERT OVERWRITE INTO {{ params.database_name }}.{{ params.schema_name }}.youtube_trending 
(video_id, title, publishedat, channelid, channeltitle, categoryid, trending_date, view_count, likes, dislikes, comment_count, comments_disabled, country)
SELECT
    * EXCLUDE(VALUE),
    REPLACE(regexp_substr_all(METADATA$FILENAME, 'youtube-trending/([A-Z]{2})')[0] , 'youtube-trending/', '') AS country
FROM {{ params.source_database_name }}.{{ params.source_schema_name }}.youtube_trending
WHERE DATE(trending_date) BETWEEN DATEADD(day, -{{ params.insert_overwrite_interval }}, DATE('{{ dag_run.logical_date }}')) AND DATE('{{ dag_run.logical_date }}')