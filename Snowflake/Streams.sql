CREATE OR REPLACE STREAM raw_table_stream ON TABLE airbnb_raw
SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM transformed_table_stream ON TABLE transformed_table
SHOW_INITIAL_ROWS = TRUE;

-- Check for NULL values in critical columns
WITH data_quality_check AS (
    SELECT *
    FROM transformed_table
    WHERE price IS NULL 
       OR minimum_nights IS NULL 
       OR availability_365 IS NULL
)

-- If rows are returned, there are data quality issues
SELECT COUNT(*) AS invalid_rows
FROM data_quality_check;