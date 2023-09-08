CREATE OR REPLACE STORAGE INTEGRATION {{ params.storage_integration_name }}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '{{ params.azure_tenant_id }}'
  STORAGE_ALLOWED_LOCATIONS = ('{{ params.youtube_storage_allowed_locations }}');