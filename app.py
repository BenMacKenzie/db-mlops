import os
from databricks import sql
from databricks.sdk.core import Config
import gradio as gr
import pandas as pd
from hello_world import create_hello_world_tab
from sales_receipts import create_sales_receipts_tab
from databricks_jobs import create_databricks_jobs_tab
from project_form import create_project_form_tab

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()
print(cfg.host)

# Query the SQL data with Service Principal credentials
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        credentials_provider=lambda: cfg.authenticate  # Uses SP credentials from the environment variables
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            df = cursor.fetchall_arrow().to_pandas()
            
            # Handle datetime columns
            datetime_columns = ['pickup_datetime', 'dropoff_datetime']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])  # Let pandas infer the format
            
            return df

# Query the SQL data with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            df = cursor.fetchall_arrow().to_pandas()
            
            # Handle datetime columns
            datetime_columns = ['pickup_datetime', 'dropoff_datetime']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])  # Let pandas infer the format
            
            return df

def get_user_token(request: gr.Request):
    # Get user token from request headers
    user_token = request.headers.get("X-Forwarded-Access-Token", "No token found")
    return user_token

def create_app():
    with gr.Blocks() as demo:
        gr.Markdown("# MLOps Dashboard")
        
        # Create tabs
        create_hello_world_tab()
        create_sales_receipts_tab()
        create_databricks_jobs_tab()
        create_project_form_tab()
        
        # Taxi Data tab
        with gr.Tab("Taxi Data"):
            gr.Textbox(value="select * from samples.nyctaxi.trips limit 5000", label="SQL Query", interactive=False)
            btn = gr.Button("Run Query")

            # store the per user session token in the state
            trigger_text = gr.Textbox(visible=False) # Hidden text to trigger rendering
            btn.click(fn=get_user_token, inputs=[], outputs=[trigger_text])

            @gr.render(inputs=[trigger_text], triggers=[trigger_text.change])
            def load_everything(user_token):
                # This example query depends on the nyctaxi data set in Unity Catalog, see https://docs.databricks.com/en/discover/databricks-datasets.html for details
                # Query the SQL data with the user credentials
                #data = sql_query_with_user_token("SELECT * FROM samples.nyctaxi.trips LIMIT 5000", user_token=user_token)
                # In order to query with Service Principal credentials, comment the above line and uncomment the below line
                data = sql_query_with_service_principal("select * from samples.nyctaxi.trips limit 50")

                with gr.Row():
                    with gr.Column(scale=3):
                        gr.Markdown("# Taxi fare distribution")
                        gr.ScatterPlot(value=data, height=400, width=700, container=False,
                                    y="fare_amount", x="trip_distance", y_title="Fare", x_title="Distance")

                gr.Dataframe(value=data)
    
    return demo

if __name__ == "__main__":
    demo = create_app()
    demo.launch(debug=True)