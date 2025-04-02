from databricks.sql import connect
import os
import pandas as pd

# Create a connection to Databricks SQL
with connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),  # Use token from environment variable
) as connection:
    with connection.cursor() as cursor:
        # Example query - replace with your actual query
        cursor.execute("select trip_distance, fare_amount from samples.nyctaxi.trips limit 50")
        
        # Fetch results
        results = cursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # If the DataFrame has datetime columns, parse them correctly
    
        # Print the DataFrame
        print(df) 