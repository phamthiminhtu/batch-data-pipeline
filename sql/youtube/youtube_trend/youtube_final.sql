INSERT OVERWRITE INTO {{ params.database_name }}.{{ params.schema_name }}.youtube_final 
(id, video_id, title, publishedat, channelid, channeltitle, categoryid, category_title, trending_date, view_count, likes, dislikes, comment_count, comments_disabled, country)
SELECT
    UUID_STRING() AS id,
    t.video_id,
    t.title,
    t.publishedat,
    t.channelid,
    t.channeltitle,
    t.categoryid,
    c.category_title,
    t.trending_date,
    CAST(t.view_count AS INT) AS view_count,
    CAST(t.likes AS INT) AS likes,
    CAST(t.dislikes AS INT) AS dislikes,
    CAST(t.comment_count AS INT) AS comment_count,
    CAST(t.comments_disabled AS BOOLEAN) AS comments_disabled,
    t.country
FROM {{ params.source_database_name }}.{{ params.source_schema_name }}.table_youtube_trending AS t
LEFT JOIN {{ params.source_database_name }}.{{ params.source_schema_name }}.table_youtube_category AS c
ON COALESCE(t.country, '') = COALESCE(c.country, '')
AND COALESCE(t.categoryid, '') = COALESCE(c.categoryid, '')
WHERE DATE(t.trending_date) BETWEEN DATEADD(day, -{{ params.insert_overwrite_interval }}, DATE('{{ dag_run.logical_date }}')) AND DATE('{{ dag_run.logical_date }}')