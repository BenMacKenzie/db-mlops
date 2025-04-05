import gradio as gr
import psycopg2
import pandas as pd

def get_postgres_data():
    """Fetch data from PostgreSQL database."""
    conn = psycopg2.connect(
        dbname="demo",
        user="ben.mackenzie",  # Your system username
        host="localhost",
        port="5432"
    )
    
    # Read data into pandas DataFrame
    df = pd.read_sql_query("SELECT * FROM users", conn)
    conn.close()
    
    return df

def create_hello_world_tab():
    with gr.Tab("PostgreSQL Data"):
        gr.Markdown("# Users Table")
        gr.Markdown("Displaying data from PostgreSQL database")
        
        # Add a refresh button
        refresh_btn = gr.Button("Refresh Data")
        
        # Display the data
        output_df = gr.Dataframe()
        
        # Function to update the dataframe
        def update_data():
            return get_postgres_data()
        
        # Set up the refresh button to update the dataframe
        refresh_btn.click(
            fn=update_data,
            inputs=[],
            outputs=[output_df]
        )
        
        # Initial load of data
        output_df.value = get_postgres_data() 