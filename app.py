import os
from databricks import sql
from databricks.sdk.core import Config
import gradio as gr
import pandas as pd
from config_manager import ConfigManager

#from sales_receipts import create_sales_receipts_tab
#from databricks_jobs import create_databricks_jobs_tab
from project_form import create_project_form_tab
#from taxi_data import create_taxi_data_tab

# Initialize configuration
config = ConfigManager()
databricks_config = config.get_databricks_config()

# Ensure required Databricks configuration is available
assert databricks_config.get('warehouse_id'), "DATABRICKS_WAREHOUSE_ID must be set in environment variables."

# Databricks config
cfg = Config(
    host=databricks_config.get('host'),
    token=databricks_config.get('token')
)
print(f"Connected to Databricks host: {cfg.host}")

def create_app():
    app_config = config.get('app', {})
    
    with gr.Blocks() as demo:
        gr.Markdown(f"# {app_config.get('name', 'MLOps Dashboard')}")
        
        # Create tabs
        #create_sales_receipts_tab()
        #create_databricks_jobs_tab()
        create_project_form_tab()
        #create_taxi_data_tab()
    
    return demo

if __name__ == "__main__":
    app_config = config.get('app', {})
    demo = create_app()
    demo.launch()
    