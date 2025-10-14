import snowflake.connector
import pyarrow as pa
import pyarrow.parquet as pq
import os
from dotenv import load_dotenv

load_dotenv()
snowflake_config = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'FEC'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS'),
}

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(**snowflake_config)
cursor = conn.cursor()

print("Querying committee_contribution_summary_view...")
query = "SELECT * FROM fec.analytics.committee_contribution_summary_view"
cursor.execute(query)

print("Fetching data...")
results = cursor.fetch_arrow_all()

output_file = 'web/data/committee_contributions.parquet'
print(f"Writing to {output_file}...")
os.makedirs('web/data', exist_ok=True)
pq.write_table(results, output_file, compression='snappy')

row_count = len(results)
print(f"\nExport complete!")
print(f"Rows exported: {row_count:,}")
print(f"File: {output_file}")

cursor.close()
conn.close()