silver_df = spark.read.format("delta").load("/mnt/delta/silver")

quality_df = silver_df.filter(col("price").isNotNull() & 
                              col("minimum_nights").isNotNull() & 
                              col("availability_365").isNotNull())

quality_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver_checked")
