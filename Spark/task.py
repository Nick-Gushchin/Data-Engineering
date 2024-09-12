from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName('Incremental_ETL').getOrCreate()

# Monitor new files in raw directory
raw_folder = '/Spark/raw/'
stream_df = spark.readStream.option("header", "true")\
                            .csv(raw_folder)

# Define a micro-batch trigger every 10 seconds
stream_df.writeStream.format("console")\
                     .trigger(processingTime="10 seconds")\
                     .start()

# Stream DataFrame that reads in new CSVs
stream_df = spark.readStream.option("header", "true")\
                            .csv(raw_folder)

processed_df = spark.read.parquet('/Spark/processed/')

merged_df = new_data_df.unionByName(processed_df)\
                       .dropDuplicates(['host_id', 'latitude', 'longitude', 'room_type', 'price'])

# Get list of new files
processed_files = get_processed_files()
new_files = [f for f in os.listdir(raw_folder) if f not in processed_files]

# Process each new file, then log it
for file_name in new_files:
    # Process file
    update_processed_files(file_name)

# Filter out rows with non-positive prices
cleaned_df = stream_df.filter("price > 0")

# Convert 'last_review' to date and fill missing values
from pyspark.sql.functions import to_date, col

cleaned_df = cleaned_df.withColumn('last_review', to_date(col('last_review')))
cleaned_df = cleaned_df.na.fill({'last_review': '1900-01-01', 'reviews_per_month': 0})

# Drop rows with missing latitude or longitude
cleaned_df = cleaned_df.dropna(subset=['latitude', 'longitude'])`

# Add price range column
cleaned_df = cleaned_df.withColumn('price_range',
  when(col('price') < 50, 'budget')
  .when(col('price') < 200, 'mid-range')
  .otherwise('luxury'))

# Add price per review column
cleaned_df = cleaned_df.withColumn('price_per_review', col('price') / col('reviews_per_month'))

cleaned_df.createOrReplaceTempView('listings')

# SQL Query 1: Listings by neighborhood group
query_1 = spark.sql("SELECT neighbourhood_group, COUNT(*) FROM listings GROUP BY neighbourhood_group ORDER BY COUNT(*) DESC")

# SQL Query 2: Top 10 most expensive listings
query_2 = spark.sql("SELECT * FROM listings ORDER BY price DESC LIMIT 10")

# SQL Query 3: Average price by room type
query_3 = spark.sql("SELECT neighbourhood_group, room_type, AVG(price) FROM listings GROUP BY neighbourhood_group, room_type")

repartitioned_df = cleaned_df.repartition('neighbourhood_group')
repartitioned_df.write.parquet('/Spark/processed/', mode='overwrite', partitionBy='neighbourhood_group')

expected_count = repartitioned_df.count()
processed_count = cleaned_df.count()

assert expected_count == processed_count, "Row count mismatch"
assert cleaned_df.filter(col('price').isNull()).count() == 0, "Null values found in price column"
assert cleaned_df.filter(col('minimum_nights').isNull()).count() == 0, "Null values found in minimum_nights column"
assert cleaned_df.filter(col('availability_365').isNull()).count() == 0, "Null values found in availability_365 column"