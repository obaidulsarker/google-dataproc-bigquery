from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName('1.2. BigQuery Storage & Spark SQL - Python')\
  .config('spark.jars', 'gs://dataproc-staging-us-central1-462289895818-0vuu3jfy/spark-lib/spark-bigquery-with-dependencies_2.13-0.29.0.jar') \
  .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled",True)

table = "project.data.path"
df_wiki_pageviews = spark.read \
  .format("bigquery") \
  .option("table", table) \
  .load()

df_wiki_pageviews.createOrReplaceTempView("view_name")

df_wiki_en = spark.sql("""
FROM view_name
WHERE var1 = 'aaaaaaa'
AND var2 >= 0.995
AND var3 <= 1.005
""").cache()