import requests
import json
import os
from typing import Optional

class DatabricksRESTClient:
    def __init__(self):
        """Initialize the Databricks REST client using environment variables."""
        self.domain = os.getenv('DATABRICKS_HOST')
        self.token = os.getenv('DATABRICKS_TOKEN')
        if not self.domain or not self.token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
        
        # Ensure domain starts with https:// and remove any trailing slashes
        self.domain = self.domain.rstrip('/')
        if not self.domain.startswith('https://'):
            self.domain = f"https://{self.domain}"
        
        self.base_url = f"{self.domain}/api/2.0"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        print(f"Initialized client with base URL: {self.base_url}")

    def create_job(self, 
                  job_name: str,
                  notebook_path: str,
                  experiment_name: str,
                  target: str,
                  table_name: str,
                  git_url: str,
                  git_provider: str,
                  git_branch: str) -> dict:
        """
        Create a Databricks job using the REST API.
        
        Args:
            job_name: Name of the job
            notebook_path: Path to the training notebook
            experiment_name: Name of the MLflow experiment
            target: Name of the target column
            table_name: Name of the input table
            git_url: URL of the Git repository
            git_provider: Git provider (e.g., "gitHub", "gitLab", "bitbucketCloud", "azureDevOpsServices")
            git_branch: Git branch to use
            
        Returns:
            The created job object
        """
        payload = {
            "name": job_name,
            "tasks": [{
                "task_key": "MyTask",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "experiment_name": experiment_name,
                        "target": target,
                        "table_name": table_name
                    }
                },
                "description": "train model"
            }],
            "settings": {
                "git_source": {
                    "url": git_url,
                    "provider": git_provider,
                    "branch": git_branch
                }
            }
        }
        
        url = f"{self.base_url}/jobs/create"
        print(f"Creating job with URL: {url}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(
            url,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code != 200:
            print(f"Error response: {response.status_code}")
            print(f"Response body: {response.text}")
            response.raise_for_status()
            
        return response.json()

    def get_job(self, job_id: str) -> Optional[dict]:
        """
        Get a job by ID using the REST API.
        
        Args:
            job_id: The ID of the job to retrieve
            
        Returns:
            The job object if it exists, None otherwise
        """
        try:
            url = f"{self.base_url}/jobs/get"
            params = {"job_id": job_id}
            print(f"Getting job with URL: {url}")
            print(f"Parameters: {params}")
            
            response = requests.get(
                url,
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 404 or (response.status_code == 400 and "does not exist" in response.text):
                print(f"Job {job_id} not found")
                return None
                
            if response.status_code != 200:
                print(f"Error response: {response.status_code}")
                print(f"Response body: {response.text}")
                response.raise_for_status()
                
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {str(e)}")
            if "does not exist" in str(e):
                return None
            raise

    def run_job(self, job_id: str) -> dict:
        """
        Run a job using the REST API.
        
        Args:
            job_id: The ID of the job to run
            
        Returns:
            The run object
        """
        url = f"{self.base_url}/jobs/run-now"
        payload = {"job_id": job_id}
        print(f"Running job with URL: {url}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(
            url,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code != 200:
            print(f"Error response: {response.status_code}")
            print(f"Response body: {response.text}")
            response.raise_for_status()
            
        return response.json()

def main():
    # Initialize the Databricks REST client
    client = DatabricksRESTClient()
    
    # Job configuration
    job_id = "221470595518931"
    job_config = {
        "job_name": "DB Trainer 4",
        "notebook_path": "/notebooks/01_Build_Model",
        "experiment_name": "titanic_4",
        "target": "Survived",
        "table_name": "benmackenzie_catalog.default.titanic",
        "git_url": "https://github.com/BenMacKenzie/db-model-trainer.git",
        "git_provider": "gitHub",
        "git_branch": "main"
    }

    # Check if job exists
    existing_job = client.get_job(job_id)
    if existing_job:
        print(f"Job with ID {job_id} already exists.")
        job_to_run = existing_job
    else:
        # Create new job
        print("Creating new job...")
        try:
            new_job = client.create_job(**job_config)
            print(f"Job created successfully with ID: {new_job['job_id']}")
            job_to_run = new_job
        except Exception as e:
            print(f"Error creating job: {str(e)}")
            return

    # Run the job
    try:
        print("Starting job run...")
        run = client.run_job(job_to_run['job_id'])
        print(f"Job run started with run ID: {run['run_id']}")
    except Exception as e:
        print(f"Error running job: {str(e)}")

if __name__ == "__main__":
    main() 