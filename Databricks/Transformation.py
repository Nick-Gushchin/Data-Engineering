# Read from Bronze Table
bronze_df = spark.read.format("delta").load("/mnt/delta/bronze")

# Data Transformation
transformed_df = (bronze_df
    .filter(col("price") > 0)  # Filter out rows where price <= 0
    .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd"))  # Convert last_review to date
    .fillna({"last_review": "1900-01-01", "reviews_per_month": 0})  # Fill missing dates and reviews_per_month
    .dropna(subset=["latitude", "longitude"])  # Drop rows with missing latitude or longitude
)

# Write Transformed Data to Silver Table
transformed_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver")
