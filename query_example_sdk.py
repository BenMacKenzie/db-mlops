from databricks.sdk.core import Config
from databricks.sql import connect
import pandas as pd

# Initialize Databricks config
cfg = Config()
print(f"Using Databricks host: {cfg.host}")

# Create a connection to Databricks SQL
with connect(
    server_hostname=cfg.host,
    http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
    credentials_provider=lambda: cfg.authenticate  # Uses credentials from the environment
) as connection:
    with connection.cursor() as cursor:
        # Example query - replace with your actual query
        cursor.execute("select trip_distance, fare_amount from samples.nyctaxi.trips limit 50")
        
        # Fetch results
        results = cursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Print the DataFrame
        print(df) 