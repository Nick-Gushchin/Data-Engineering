CREATE OR REPLACE TASK daily_data_transformation_task
  WAREHOUSE = 'SQL'
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Runs at midnight UTC every day
AS
  -- Transformation process
  CREATE OR REPLACE TABLE transformed_table AS
  WITH filtered_data AS (
      SELECT *
      FROM your_table
      WHERE price > 0
  ),
  
  last_review_transformed AS (
      SELECT *,
             COALESCE(TRY_TO_DATE(last_review), 
                      (SELECT MIN(TRY_TO_DATE(last_review)) FROM filtered_data)) AS last_review_cleaned
      FROM filtered_data
  ),
  
  reviews_handled AS (
      SELECT *,
             COALESCE(reviews_per_month, 0) AS reviews_per_month_cleaned
      FROM last_review_transformed
  )
  
  SELECT *
  FROM reviews_handled
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

  ALTER TASK daily_data_transformation_task RESUME;