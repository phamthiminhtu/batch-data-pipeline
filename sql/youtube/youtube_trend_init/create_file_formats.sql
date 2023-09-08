CREATE OR REPLACE FILE FORMAT {{ params.database_name }}.{{ params.schema_name }}.my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','  -- Specify the field delimiter used in the CSV
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Raise an error if the number of columns in a row does not match the header
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER = 1;

/* Create file format for json files*/
CREATE OR REPLACE FILE FORMAT {{ params.database_name }}.{{ params.schema_name }}.my_json_format
  TYPE = JSON
  TRIM_SPACE = TRUE;