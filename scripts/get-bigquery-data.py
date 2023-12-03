from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
'key\gcp-service-acc-api-key.json')

project_id = 'ancient-booster-380005'
client = bigquery.Client(credentials= credentials,project=project_id)


query_job = client.query("""
   SELECT *
   FROM `ancient-booster-380005.obaidul.viewtest`
   LIMIT 1000 """)

results = query_job.result() # Wait for the job to complete.

print("Total rows fetched: ", results.total_rows)
