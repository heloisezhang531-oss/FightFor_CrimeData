#API-Chicago-request
import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
from sodapy import Socrata


# 1. Fetch data from Chicago data portal
SOCRATA_DOMAIN = "data.cityofchicago.org"
DATASET_ID = "ijzp-q8t2"
APP_TOKEN = "cqprHN60l59KfgO51lLfqUl1N"  
client = Socrata(SOCRATA_DOMAIN,APP_TOKEN)

all_batches = []
for i in range(10):
    year = 2015 + i
    where_clause = f"date > '{year}-01-01T00:00:00' and date < '{year}-12-31T23:59:59'"
    results = client.get(DATASET_ID,where=where_clause, 
                        limit=1000, order="date ASC")
    df = pd.DataFrame.from_records(results)
    all_batches.append(df)

final_df = pd.concat(all_batches, ignore_index=True)

# 2. Connect to TiDB database
conn_str = "mysql://359KtARLdvjZ8XK.root:zm3W9Rbd6cUUHxge@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/test"
engine = create_engine(conn_str)

# 3. 写入 (if_exists='append' 方便多次运行)
df.to_sql("chicago_crimes", engine, if_exists='append', index=False)