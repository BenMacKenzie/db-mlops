import requests
import os
from databricks.sdk import WorkspaceClient

def get_experiment_runs(experiment_id, host, token):
    """
    Get all runs for a specific experiment.
    
    Args:
        experiment_id (str): The ID of the experiment
        host (str): Databricks workspace host
        token (str): Databricks access token
        
    Returns:
        list: List of runs or None if error occurs
    """
    try:
        # Construct the API URL
        url = f"{host}/api/2.0/mlflow/runs/search"
        
        # Set up the headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Set up the request body
        data = {
            "experiment_ids": [experiment_id],
            "max_results": 1000  # Adjust this if you need more runs
        }
        
        # Make the API call
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            runs_data = response.json()
            return runs_data.get('runs', [])
        else:
            print(f"Error fetching runs: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Error getting runs: {str(e)}")
        return None

def get_experiment_by_name(experiment_name):
    """
    Get experiment details by name using the Databricks REST API.
    
    Args:
        experiment_name (str): The name of the experiment to retrieve
        
    Returns:
        dict: Experiment details or error message
    """
    try:
        # Get the host and token from environment variables
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        
        if not host or not token:
            print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
            return None
        
        # Construct the API URL
        url = f"{host}/api/2.0/mlflow/experiments/get-by-name"
        
        # Set up the headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        experiment_name = f"/Users/ben.mackenzie@databricks.com/{experiment_name}"
        
        # Set up the request body
        data = {
            "experiment_name": experiment_name
        }
        
        # Make the API call
        response = requests.get(url, headers=headers, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            experiment = response.json()['experiment']
            print("\nExperiment Details:")
            print(f"Experiment ID: {experiment['experiment_id']}")
            print(f"Name: {experiment['name']}")
            print(f"Artifact Location: {experiment['artifact_location']}")
            print(f"Lifecycle Stage: {experiment['lifecycle_stage']}")
            print(f"Last Update Time: {experiment['last_update_time']}")
            
            # Get and display runs for the experiment
            print("\nExperiment Runs:")
            runs = get_experiment_runs(experiment['experiment_id'], host, token)
            if runs:
                for i, run in enumerate(runs, 1):
                    print(f"\nRun {i}:")
                    print(f"  Run ID: {run['info']['run_id']}")
                    print(f"  Status: {run['info']['status']}")
                    print(f"  Start Time: {run['info']['start_time']}")
                    print(f"  End Time: {run['info'].get('end_time', 'N/A')}")
                    
                    # Print metrics if available
                    if run.get('data', {}).get('metrics'):
                        print("  Metrics:")
                        for metric in run['data']['metrics']:
                            print(f"    {metric['key']}: {metric['value']}")
                    
                    # Print parameters if available
                    if run.get('data', {}).get('params'):
                        print("  Parameters:")
                        for param in run['data']['params']:
                            print(f"    {param['key']}: {param['value']}")
            else:
                print("No runs found for this experiment")
            
            return experiment
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Error getting experiment: {str(e)}")
        return None

if __name__ == "__main__":
    # Example usage
    experiment_name = input("Enter experiment name: ")
    get_experiment_by_name(experiment_name) 