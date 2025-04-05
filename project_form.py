import gradio as gr
import psycopg2
from psycopg2 import sql
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task, GitSource, GitProvider
import requests

def get_experiment_runs(experiment_id, host, token):
    """Get all runs for a specific experiment."""
    try:
        url = f"{host}/api/2.0/mlflow/runs/search"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {
            "experiment_ids": [experiment_id],
            "max_results": 1000
        }
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            runs_data = response.json()
            return runs_data.get('runs', [])
        else:
            return f"Error fetching runs: {response.status_code} - {response.text}"
            
    except Exception as e:
        return f"Error getting runs: {str(e)}"

def get_experiment_details(project_name):
    """Get experiment details and runs."""
    try:
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        
        if not host or not token:
            return "Error: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set"
        
        # Format experiment name
        experiment_name = f"/Users/ben.mackenzie@databricks.com/{project_name}"
        
        # Get experiment details
        url = f"{host}/api/2.0/mlflow/experiments/get-by-name"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {"experiment_name": experiment_name}
        
        response = requests.get(url, headers=headers, json=data)
        
        if response.status_code != 200:
            return f"Error getting experiment: {response.status_code} - {response.text}"
        
        experiment = response.json()['experiment']
        
        # Format experiment details
        details = [
            f"Experiment ID: {experiment['experiment_id']}",
            f"Name: {experiment['name']}",
            f"Artifact Location: {experiment['artifact_location']}",
            f"Lifecycle Stage: {experiment['lifecycle_stage']}",
            f"Last Update Time: {experiment['last_update_time']}",
            "\nRuns:"
        ]
        
        # Get and format runs
        runs = get_experiment_runs(experiment['experiment_id'], host, token)
        if isinstance(runs, list):
            for i, run in enumerate(runs, 1):
                details.append(f"\nRun {i}:")
                details.append(f"  Run ID: {run['info']['run_id']}")
                details.append(f"  Status: {run['info']['status']}")
                details.append(f"  Start Time: {run['info']['start_time']}")
                details.append(f"  End Time: {run['info'].get('end_time', 'N/A')}")
                
                if run.get('data', {}).get('metrics'):
                    details.append("  Metrics:")
                    for metric in run['data']['metrics']:
                        details.append(f"    {metric['key']}: {metric['value']}")
                
                if run.get('data', {}).get('params'):
                    details.append("  Parameters:")
                    for param in run['data']['params']:
                        details.append(f"    {param['key']}: {param['value']}")
        else:
            details.append(runs)  # This will be the error message if runs fetch failed
        
        return "\n".join(details)
        
    except Exception as e:
        return f"Error: {str(e)}"

def create_project_form_tab():
    """Create a tab with a form to collect project information."""
    
    def check_and_update_table():
        """Check if job_id column exists and add it if not."""
        try:
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                # Check if job_id column exists
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'projects' 
                    AND column_name = 'job_id'
                """)
                if not cur.fetchone():
                    # Add job_id column if it doesn't exist
                    cur.execute("ALTER TABLE projects ADD COLUMN job_id VARCHAR(255)")
                    conn.commit()
                    print("Added job_id column to projects table")
            
            conn.close()
        except Exception as e:
            print(f"Error checking/updating table structure: {str(e)}")
    
    # Check and update table structure when the tab is created
    check_and_update_table()
    
    def get_projects():
        """Fetch all projects from the database."""
        try:
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                cur.execute("SELECT id, project_name FROM projects ORDER BY project_name")
                projects = cur.fetchall()
            
            conn.close()
            # Return both the display name (project_name) and the value (id:project_name)
            return [(p[1], f"{p[0]}:{p[1]}") for p in projects] if projects else []
            
        except Exception as e:
            print(f"Error fetching projects: {str(e)}")
            return []
    
    def get_project_details(project_str):
        """Fetch details of a specific project."""
        try:
            project_id = project_str.split(":")[0]
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                cur.execute("SELECT project_name, table_name, target, job_id FROM projects WHERE id = %s", (project_id,))
                project = cur.fetchone()
            
            conn.close()
            return project if project else (None, None, None, None)
            
        except Exception as e:
            print(f"Error fetching project details: {str(e)}")
            return (None, None, None, None)
    
    def delete_project(project_id):
        """Delete a project from the database."""
        try:
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                cur.execute("DELETE FROM projects WHERE id = %s", (project_id,))
            
            conn.commit()
            conn.close()
            return "Project deleted successfully!"
            
        except Exception as e:
            return f"Error deleting project: {str(e)}"
    
    def train_model(project_id, project_name, table_name, target):
        """Create or use existing Databricks job for model training."""
        try:
            # Validate that a project is selected
            if not project_id:
                return "Please select a project first"
            
            # Initialize the Databricks Workspace client
            workspace_client = WorkspaceClient()
            
            # Check if job_id exists in the database
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                cur.execute("SELECT job_id FROM projects WHERE id = %s", (project_id,))
                result = cur.fetchone()
                existing_job_id = result[0] if result else None
            
            if existing_job_id:
                # Use existing job
                try:
                    existing_job = workspace_client.jobs.get(job_id=existing_job_id)
                    job_to_run = existing_job
                    message = f"Using existing job with ID: {existing_job_id}"
                except Exception as e:
                    return f"Error accessing existing job: {str(e)}"
            else:
                # Create new job
                try:
                    serverless_job = workspace_client.jobs.create(
                        name=project_name,
                        git_source=GitSource(
                            git_url="https://github.com/BenMacKenzie/db-model-trainer",
                            git_provider=GitProvider("gitHub"),
                            git_branch="main"
                        ),
                        tasks=[
                            Task(
                                task_key="MyTask",
                                notebook_task=NotebookTask(
                                    notebook_path="notebooks/01_Build_Model",
                                    base_parameters={
                                        "experiment_name": project_name,
                                        "target": target,
                                        "table_name": table_name
                                    }
                                ),
                                description="train model"
                            )
                        ]
                    )
                    job_to_run = serverless_job
                    message = f"Created new job with ID: {serverless_job.job_id}"
                    
                    # Store the new job_id in the database
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE projects SET job_id = %s WHERE id = %s",
                            (serverless_job.job_id, project_id)
                        )
                    conn.commit()
                    
                except Exception as e:
                    return f"Error creating new job: {str(e)}"
            
            # Run the job
            run = workspace_client.jobs.run_now(job_id=job_to_run.job_id)
            message += f"\nJob run started with run ID: {run.run_id}"
            
            conn.close()
            return message
            
        except Exception as e:
            return f"Error in training process: {str(e)}"
    
    def save_project_info(project_name, table_name, target, project_id=None):
        """Save project information to PostgreSQL database."""
        try:
            conn = psycopg2.connect(
                dbname="demo",
                user="ben.mackenzie",
                host="localhost",
                port="5432"
            )
            
            with conn.cursor() as cur:
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS projects (
                        id SERIAL PRIMARY KEY,
                        project_name VARCHAR(255) NOT NULL,
                        table_name VARCHAR(255) NOT NULL,
                        target VARCHAR(255) NOT NULL,
                        job_id VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                if project_id:
                    # Update existing project
                    cur.execute(
                        "UPDATE projects SET project_name = %s, table_name = %s, target = %s WHERE id = %s",
                        (project_name, table_name, target, project_id)
                    )
                    message = "Project updated successfully!"
                else:
                    # Insert new project
                    cur.execute(
                        "INSERT INTO projects (project_name, table_name, target) VALUES (%s, %s, %s)",
                        (project_name, table_name, target)
                    )
                    message = "Project created successfully!"
                
            conn.commit()
            conn.close()
            return message
            
        except Exception as e:
            return f"Error saving project information: {str(e)}"
    
    with gr.Tab("Project Form"):
        gr.Markdown("## Project Management")
        
        with gr.Row():
            # Left column - Project List
            with gr.Column(scale=1):
                gr.Markdown("### Projects")
                project_dropdown = gr.Dropdown(
                    choices=get_projects(),
                    label="Select Project",
                    interactive=True
                )
                gr.Markdown("---")
                gr.Markdown("### Actions")
                with gr.Row():
                    create_btn = gr.Button("New Project")
                    clear_btn = gr.Button("Clear Form")
                    delete_btn = gr.Button("Delete Project")
            
            # Right column - Form
            with gr.Column(scale=2):
                gr.Markdown("### Project Details")
                project_name = gr.Textbox(label="Project Name", placeholder="Enter project name")
                table_name = gr.Textbox(label="Table Name", placeholder="Enter table name")
                target = gr.Textbox(label="Target", placeholder="Enter target column name")
                
                # Hidden field for project ID
                project_id = gr.Textbox(visible=False)
                
                with gr.Row():
                    update_btn = gr.Button("Update Project")
                    train_btn = gr.Button("Train Model")
                    view_experiment_btn = gr.Button("View Experiment")
                
                output = gr.Textbox(label="Status")
                experiment_output = gr.Textbox(label="Experiment Details", lines=10)
        
        def load_project(project_str):
            if not project_str:
                return "", "", "", ""
            details = get_project_details(project_str)
            if details[0] is None:  # If project not found
                return "", "", "", ""
            return project_str.split(":")[0], details[0], details[1], details[2]
        
        def clear_form():
            return "", "", "", ""
        
        # Set up button click handlers
        project_dropdown.change(
            fn=load_project,
            inputs=[project_dropdown],
            outputs=[project_id, project_name, table_name, target]
        )
        
        create_btn.click(
            fn=lambda: ("", "", "", ""),
            inputs=[],
            outputs=[project_id, project_name, table_name, target]
        )
        
        clear_btn.click(
            fn=clear_form,
            inputs=[],
            outputs=[project_id, project_name, table_name, target]
        )
        
        update_btn.click(
            fn=save_project_info,
            inputs=[project_name, table_name, target, project_id],
            outputs=[output]
        )
        
        create_btn.click(
            fn=save_project_info,
            inputs=[project_name, table_name, target],
            outputs=[output]
        )
        
        def delete_and_refresh(project_id):
            message = delete_project(project_id)
            return message, gr.update(choices=get_projects(), value="")
        
        delete_btn.click(
            fn=delete_and_refresh,
            inputs=[project_id],
            outputs=[output, project_dropdown]
        )
        
        train_btn.click(
            fn=train_model,
            inputs=[project_id, project_name, table_name, target],
            outputs=[output]
        )
        
        view_experiment_btn.click(
            fn=get_experiment_details,
            inputs=[project_name],
            outputs=[experiment_output]
        )
        
        # Refresh project list after any action
        def refresh_projects():
            return gr.update(choices=get_projects())
        
        update_btn.click(
            fn=refresh_projects,
            inputs=[],
            outputs=[project_dropdown]
        )
        
        create_btn.click(
            fn=refresh_projects,
            inputs=[],
            outputs=[project_dropdown]
        ) 