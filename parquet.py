import pandas as pd
from pathlib import Path
union_dfs = []

# Loop through parquet file
for f in Path(".").glob("*.parquet"):
    print(f)
    # read each files
    df = pd.read_parquet(f)
    # add a new column capturing file name
    df["filename"] = f.name
    # storing the temporary dataframe to concat
    union_dfs.append(df)
    print(df.tail())
comb_df = pd.concat(union_dfs, ignore_index=True)
print(comb_df.tail())
#export to Csv to test
comb_df.to_csv("all.csv")
#Initialize the BigQuery client, datsets and tables
client = bigquery.Client(project ='enter project name')
table_id = 'dataset_name.table_name'
# Load to BigQuery (append if the table salready exists)
comb_df.to_gbq(table_id, if exists = 'append')