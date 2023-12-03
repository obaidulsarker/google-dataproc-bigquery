# spark library
from pyspark.sql import SparkSession

# bigquery library
from google.cloud import bigquery
from google.oauth2 import service_account

# GCP Credential - store in GCP VM where dataproc is installed
credentials = service_account. \
    Credentials. \
    from_service_account_file("/home/osarker2023/gcp-service-acc-api-key.json")

project_id = 'ancient-booster-380005'
bq_dataset = "obaidul"

# bigquery Connection
client = bigquery.Client(credentials=credentials, project=project_id)

#create spark session
spark = SparkSession.builder.master('yarn')\
  .appName('spark-read-from-bigquery')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
  .getOrCreate()

# table name and dataset name
tbl_name = "ancient-booster-380005.obaidul.testdata"
ds_name = "ancient-booster-380005.obaidul"

# Enable view
spark.conf.set("viewEnabled","true")
spark.conf.set("materializationDataset",ds_name)

# GCP bucket
bucket = 'dataproc-staging-us-central1-462289895818-0vuu3jfy'

# Read BigQuery table and load it into Spark dataframe
df = spark.read.format("bigquery") \
  .option("credentialsFile","/home/osarker2023/gcp-service-acc-api-key.json") \
  .option("table",tbl_name)\
  .option("filter", "Day >= '2023-01-01' AND Day < '2023-03-02'") \
  .load()
 
# print the dataframe with 2 records
df.limit(2).show()
 
# print schema of the dataframe
df.printSchema()

# Create temp table
df.createOrReplaceTempView("new_view2")

# Save dataframe to new table
new_table = "ancient-booster-380005.obaidul.new_testdata2"
spark.conf.set("temporaryGcsBucket",bucket)
df.write \
  .format("bigquery") \
  .save(new_table)

# new dataset
shared_dataset_id = "shared_views"
shared_dataset_id_full = "{}.{}".format(project_id, shared_dataset_id)

# create dataset
shared_dataset = bigquery.Dataset(shared_dataset_id_full)
shared_dataset.location = "US"
shared_dataset = client.create_dataset(shared_dataset)  # API request

# Create the view in the new dataset
shared_view_id = "new_view"
view = bigquery.Table(shared_dataset.table(shared_view_id))
sql_template = """
    SELECT *
  FROM ancient-booster-380005.obaidul.testdata
  WHERE Day >= '2023-01-01' AND Day < '2023-03-02'
"""
view.view_query = sql_template

view = client.create_table(view)  # API request