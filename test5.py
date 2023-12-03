#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('local') \
  .appName('spark-bigquery-demo') \
  .config('spark.jars', 'lib/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = 'dataproc-staging-us-central1-462289895818-0vuu3jfy'
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'ancient-booster-380005.obaidul.viewtest') \
  .load()

# Create temp table
words.createOrReplaceTempView('new_viewtest')

# Perform word count.
word_count = spark.sql(
    'SELECT * FROM ancient-booster-380005.obaidul.new_viewtest WHERE rank=1')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'obaidul.new_viewtest_output') \
  .save()