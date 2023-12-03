from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
import db_dtypes

credentials = service_account. \
    Credentials. \
    from_service_account_file('key\gcp-service-acc-api-key.json')
 
 # setup
project_id = 'ancient-booster-380005'
bq_dataset = "obaidul"

# Connection
client = bigquery.Client(credentials=credentials, project=project_id)
dataset_ref = client.dataset(bq_dataset)

# Result to dataframe function
def gcp2df(sql):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()


v_sql = """ 
SELECT *
   FROM ancient-booster-380005.obaidul.viewtest """

df =  gcp2df(v_sql)

print(df.head)
