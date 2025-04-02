from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task, GitSource, GitProvider

def create_training_job(workspace_client, job_name, notebook_path, experiment_name, target, table_name, git_url, git_provider, git_branch):
    """
    Create a Databricks training job with the specified parameters.
    
    Args:
        workspace_client: Databricks WorkspaceClient instance
        job_name: Name of the job
        notebook_path: Path to the training notebook
        experiment_name: Name of the MLflow experiment
        target_column: Name of the target column
        table_name: Name of the input table
        git_url: URL of the Git repository
        git_provider: Git provider (e.g., "gitHub", "gitLab", "bitbucketCloud", "azureDevOpsServices")
        git_branch: Git branch to use
    
    Returns:
        The created job object
    """
    return workspace_client.jobs.create(
        name=job_name,
        git_source=GitSource(
                        git_url=git_url,
                        git_provider=GitProvider(git_provider),
                        git_branch=git_branch
                    ),
        tasks=[
            Task(
                task_key="MyTask",
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    
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
job_id = "1333963516113230"
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
        job_name="DB Trainer 6",
        notebook_path="notebooks/01_Build_Model",
        experiment_name="titanic_5",
        target="Survived",
        table_name="benmackenzie_catalog.default.titanic",
        git_url="https://github.com/BenMacKenzie/db-model-trainer",
        git_provider="gitHub",
        git_branch="main"
    )

    # Print the job details
    print(f"Serverless job created successfully with ID: {serverless_job.job_id}")
    job_to_run = serverless_job

# Run the job
print(f"Starting job run...")
run = workspace_client.jobs.run_now(job_id=job_to_run.job_id)
print(f"Job run started with run ID: {run.run_id}")