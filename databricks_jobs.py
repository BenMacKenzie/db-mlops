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
                
                # Source selection
                source_type = gr.Radio(
                    choices=["Workspace", "Git"],
                    label="Notebook Source",
                    value="Workspace"
                )
                
                # Workspace notebook path
                notebook_path = gr.Textbox(
                    label="Workspace Notebook Path",
                    placeholder="/path/to/your/notebook",
                    visible=True
                )
                
                # Git notebook details
                git_url = gr.Textbox(
                    label="Git Repository URL",
                    placeholder="https://github.com/username/repo.git",
                    visible=False
                )
                
                git_branch = gr.Textbox(
                    label="Git Branch",
                    placeholder="main",
                    visible=False
                )
                
                git_path = gr.Textbox(
                    label="Notebook Path in Git",
                    placeholder="notebooks/example.ipynb",
                    visible=False
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
        
        def update_visibility(source):
            """Update visibility of notebook path fields based on source selection."""
            if source == "Workspace":
                return {
                    notebook_path: gr.update(visible=True),
                    git_url: gr.update(visible=False),
                    git_branch: gr.update(visible=False),
                    git_path: gr.update(visible=False)
                }
            else:
                return {
                    notebook_path: gr.update(visible=False),
                    git_url: gr.update(visible=True),
                    git_branch: gr.update(visible=True),
                    git_path: gr.update(visible=True)
                }
        
        def create_job(name, source, notebook, git_url, git_branch, git_path, params):
            """Create a new Databricks job with specified parameters."""
            if not name:
                return "Please enter a job name"
            
            if source == "Workspace" and not notebook:
                return "Please enter a workspace notebook path"
            
            if source == "Git" and not all([git_url, git_branch, git_path]):
                return "Please fill in all Git-related fields"
            
            try:
                # Parse the parameters JSON
                try:
                    params_dict = json.loads(params)
                except json.JSONDecodeError:
                    return "Error: Invalid JSON format in parameters"
                
                w = WorkspaceClient()
                
                # Prepare the task configuration
                task_config = {
                    "task_key": "notebook_task",
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "Standard_DS3_v2",
                        "num_workers": 1,
                        "spark_conf": {
                            "spark.databricks.cluster.profile": "serverless"
                        }
                    }
                }
                
                # Add notebook task configuration based on source
                if source == "Workspace":
                    task_config["notebook_task"] = {
                        "notebook_path": notebook,
                        "source": "WORKSPACE",
                        "base_parameters": params_dict
                    }
                else:  # Git source
                    task_config["git_source"] = {
                        "git_url": git_url,
                        "git_provider": "gitHub",
                        "branch": git_branch
                    }
                    task_config["notebook_task"] = {
                        "notebook_path": git_path,
                        "source": "GIT",
                        "base_parameters": params_dict
                    }
                
                # Create the job using the jobs service directly
                job = w.jobs.create(
                    name=name,
                    tasks=[task_config]
                )
                
                # Convert the job response to a dictionary if it's not already
                if hasattr(job, '__dict__'):
                    job_dict = job.__dict__
                else:
                    job_dict = job
                
                # Get the job ID from the dictionary
                job_id = job_dict.get('job_id', 'Unknown')
                return f"Job created successfully!\nJob ID: {job_id}"
                
            except Exception as e:
                return f"Error creating job: {str(e)}"
        
        # Set up the source type radio to update visibility
        source_type.change(
            fn=update_visibility,
            inputs=[source_type],
            outputs=[notebook_path, git_url, git_branch, git_path]
        )
        
        # Set up the create button
        create_btn.click(
            fn=create_job,
            inputs=[job_name, source_type, notebook_path, git_url, git_branch, git_path, parameters],
            outputs=[status_text]
        ) 