from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task, Source

def create_training_job(workspace_client, job_name, notebook_path, experiment_name, target, table_name):
    """
    Create a Databricks training job with the specified parameters.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        job_name: Name of the job
        notebook_path: Path to the training notebook
        experiment_name: Name of the MLflow experiment
        target_column: Name of the target column
        table_name: Name of the input table
    
    Returns:
        The created job object
    """
    return workspace_client.jobs.create(
        name=job_name,
        tasks=[
            Task(
                task_key="MyTask",
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    source=Source("WORKSPACE"),
                    base_parameters={
                        "experiment_name": experiment_name,
                        "target": target,
                        "table_name": table_name
                    }
                ),
                description="train model"
                # No cluster configuration is specified, making it serverless by default
            )
        ]
    )

# Initialize the Databricks Workspace client
job_id = "221470595518931"
workspace_client = WorkspaceClient()

# Check if job exists
try:
    existing_job = workspace_client.jobs.get(job_id=job_id)
    print(f"Job with ID {job_id} already exists.")
    job_to_run = existing_job
except Exception as e:
    # If job doesn't exist, create it
    serverless_job = create_training_job(
        workspace_client=workspace_client,
        job_name="DB Trainer 2",
        notebook_path="/Workspace/Users/ben.mackenzie@databricks.com/db-model-trainer/notebooks/01_Build_Model",
        experiment_name="titanic_4",
        target="Survived",
        table_name="benmackenzie_catalog.default.titanic"
    )

    # Print the job details
    print(f"Serverless job created successfully with ID: {serverless_job.job_id}")
    job_to_run = serverless_job

# Run the job
print(f"Starting job run...")
run = workspace_client.jobs.run_now(job_id=job_to_run.job_id)
print(f"Job run started with run ID: {run.run_id}")