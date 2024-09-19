-- 1. Filter out rows where price is 0 or negative
WITH filtered_data AS (
    SELECT *
    FROM AIRBNB_RAW
    WHERE price > 0
),

-- 2. Convert last_review to a valid date and fill missing values with the earliest available date
last_review_transformed AS (
    SELECT *,
           COALESCE(TRY_TO_DATE(last_review), 
                    (SELECT MIN(TRY_TO_DATE(last_review)) FROM filtered_data)) AS last_review_cleaned
    FROM filtered_data
),

-- 3. Handle missing values in reviews_per_month by setting them to 0
reviews_handled AS (
    SELECT *,
           COALESCE(reviews_per_month, 0) AS reviews_per_month_cleaned
    FROM last_review_transformed
)

-- 4. Drop rows with missing latitude or longitude
SELECT *
FROM reviews_handled
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;