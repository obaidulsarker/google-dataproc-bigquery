from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .master('yarn')\
  .appName('1.2. BigQuery Storage & Spark SQL - Python')\
  .config('spark.jars', 'lib/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

# Enable repl.eagerEval
spark.conf.set("spark.sql.repl.eagerEval.enabled",True)

table = "ancient-booster-380005.obaidul.viewtest"
df_wiki_pageviews = spark.read \
  .format("bigquery") \
  .option("table", table) \
  .option("filter", "Day >= '2020-03-01' AND Day < '2020-03-02'") \
  .load()

# print shema
df_wiki_pageviews.printSchema()

# create temp table
df_wiki_pageviews.createOrReplaceTempView("wiki_pageviews")

# query
df_wiki_en = spark.sql("""
SELECT 
 title, wiki, views
FROM wiki_pageviews
WHERE views > 1000 AND wiki in ('en', 'en.m')
""").cache()

df_wiki_en.show(10)
