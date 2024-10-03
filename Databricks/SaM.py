bronze_stream = spark.readStream.format("delta").load("/mnt/delta/bronze")

transformed_stream = (bronze_stream
    .filter(col("price") > 0)
    .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd"))
    .fillna({"last_review": "1900-01-01", "reviews_per_month": 0})
    .dropna(subset=["latitude", "longitude"])
)

transformed_stream.writeStream.format("delta").outputMode("append").start("/mnt/delta/silver")
