import gradio as gr
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
import json

def create_databricks_jobs_tab():
    with gr.Tab("Databricks Jobs"):
        gr.Markdown("# Create Databricks Job")
        gr.Markdown("Specify parameters and create a new Databricks job")
        
        with gr.Row():
            with gr.Column(scale=1):
                # Job creation form
                job_name = gr.Textbox(
                    label="Job Name",
                    placeholder="Enter job name"
                )
                
                notebook_path = gr.Textbox(
                    label="Notebook Path",
                    placeholder="/path/to/your/notebook"
                )
                
                # Add more parameters as needed
                gr.Markdown("### Job Parameters")
                gr.Markdown("Enter parameters as JSON (e.g., {\"param1\": \"value1\", \"param2\": \"value2\"})")
                parameters = gr.Textbox(
                    label="Parameters",
                    value="{}",
                    lines=3
                )
                
                # Create button
                create_btn = gr.Button("Create Job")
                
            with gr.Column(scale=1):
                # Status and output
                status_text = gr.Textbox(
                    label="Status",
                    lines=5,
                    interactive=False
                )
        
        def create_job(name, notebook, params):
            """Create a new Databricks job with specified parameters."""
            if not all([name, notebook]):
                return "Please fill in all required fields (name and notebook path)"
            
            try:
                # Parse the parameters JSON
                try:
                    params_dict = json.loads(params)
                except json.JSONDecodeError:
                    return "Error: Invalid JSON format in parameters"
                
                w = WorkspaceClient()
                
                # Create the job with serverless compute
                job = w.jobs.create(
                    name=name,
                    tasks=[{
                        "task_key": "notebook_task",
                        "notebook_task": {
                            "notebook_path": notebook,
                            "source": "WORKSPACE",
                            "base_parameters": params_dict
                        },
                        "new_cluster": {
                            "spark_version": "13.3.x-scala2.12",
                            "node_type_id": "Standard_DS3_v2",
                            "num_workers": 1,
                            "spark_conf": {
                                "spark.databricks.cluster.profile": "serverless"
                            }
                        }
                    }]
                )
                
                return f"Job created successfully!\nJob ID: {job.job_id}"
                
            except Exception as e:
                return f"Error creating job: {str(e)}"
        
        # Set up the create button
        create_btn.click(
            fn=create_job,
            inputs=[job_name, notebook_path, parameters],
            outputs=[status_text]
        ) 