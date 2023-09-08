CREATE OR REPLACE STAGE {{ params.database_name }}.{{ params.schema_name }}.{{ params.youtube_stage_name }}
STORAGE_INTEGRATION = {{ params.storage_integration_name }}
URL='{{ params.youtube_storage_allowed_locations }}';
