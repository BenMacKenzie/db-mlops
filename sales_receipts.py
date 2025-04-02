import gradio as gr
import psycopg2
import pandas as pd
import yaml
from datetime import datetime

def get_sales_receipts():
    """Fetch sales receipts data from PostgreSQL database."""
    conn = psycopg2.connect(
        dbname="demo",
        user="ben.mackenzie",  # Your system username
        host="localhost",
        port="5432"
    )
    
    # Read data into pandas DataFrame
    df = pd.read_sql_query("""
        SELECT 
            id,
            receipt_data,
            created_at
        FROM sales_receipts
    """, conn)
    
    conn.close()
    
    return df

def update_receipt(receipt_id: int, yaml_data: str):
    """Update a receipt in the database."""
    conn = psycopg2.connect(
        dbname="demo",
        user="ben.mackenzie",
        host="localhost",
        port="5432"
    )
    
    try:
        # Validate YAML before updating
        yaml.safe_load(yaml_data)
        
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE sales_receipts SET receipt_data = %s WHERE id = %s",
                (yaml_data, receipt_id)
            )
        conn.commit()
        return "Receipt updated successfully!"
    except yaml.YAMLError as e:
        return f"Error: Invalid YAML format - {str(e)}"
    except Exception as e:
        return f"Error updating receipt: {str(e)}"
    finally:
        conn.close()

def create_sales_receipts_tab():
    with gr.Tab("Sales Receipts"):
        gr.Markdown("# Sales Receipts")
        gr.Markdown("Displaying sales receipt data from PostgreSQL database (stored as YAML)")
        
        with gr.Row():
            with gr.Column(scale=1):
                # Add a refresh button
                refresh_btn = gr.Button("Refresh Data")
                
                # List of receipts
                receipt_list = gr.Dropdown(
                    choices=[],
                    label="Receipts",
                    interactive=True
                )
                
            with gr.Column(scale=2):
                # Display and edit the YAML data
                output_text = gr.Textbox(
                    label="Receipt Data",
                    lines=20,
                    interactive=True
                )
                
                # Update button
                update_btn = gr.Button("Update Receipt")
                update_status = gr.Textbox(label="Status")
        
        def update_receipt_list():
            df = get_sales_receipts()
            # Create list of receipt descriptions
            receipts = []
            for _, row in df.iterrows():
                yaml_data = row['receipt_data'].encode().decode('unicode-escape')
                receipt = yaml.safe_load(yaml_data)
                # Create tuple of (label, value) for each receipt
                receipts.append((f"{receipt['store']} - {receipt['date']}", f"{row['id']}: {receipt['store']} - {receipt['date']}"))
            return receipts
        
        def display_selected_receipt(choice):
            if not choice:
                return ""
            
            # Extract receipt ID from choice
            receipt_id = int(choice.split(':')[0])
            
            # Get the receipt data
            df = get_sales_receipts()
            receipt_row = df[df['id'] == receipt_id].iloc[0]
            
            # Decode and return the YAML
            return receipt_row['receipt_data'].encode().decode('unicode-escape')
        
        def handle_update(choice, yaml_data):
            if not choice:
                return "Please select a receipt first"
            
            # Extract receipt ID from choice
            receipt_id = int(choice.split(':')[0])
            
            # Update the receipt
            return update_receipt(receipt_id, yaml_data)
        
        # Set up the refresh button to update the list
        refresh_btn.click(
            fn=update_receipt_list,
            inputs=[],
            outputs=[receipt_list]
        )
        
        # Set up the list selection to update the display
        receipt_list.change(
            fn=display_selected_receipt,
            inputs=[receipt_list],
            outputs=[output_text]
        )
        
        # Set up the update button
        update_btn.click(
            fn=handle_update,
            inputs=[receipt_list, output_text],
            outputs=[update_status]
        )
        
        # Initial load of receipt list
        receipt_list.choices = update_receipt_list() 