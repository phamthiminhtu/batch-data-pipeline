CREATE OR REPLACE EXTERNAL TABLE {{ params.database_name }}.{{ params.schema_name }}.youtube_category
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
        LOCATION=>'@{{ params.youtube_stage_name }}/youtube-category',
        FILE_FORMAT=>'my_json_format'
        )
    )
    )
    LOCATION=@{{ params.youtube_stage_name }}/youtube-category
    FILE_FORMAT=my_json_format
    AUTO_REFRESH=false;