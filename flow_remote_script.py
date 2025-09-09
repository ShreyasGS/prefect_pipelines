import os
import dlt
from prefect import flow, task
from prefect_github import GitHubCredentials
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse

def set_github_pat_env():
    # GitHub PAT (GitHubCredentials.token is SecretStr -> use .get_secret_value())
    pat = GitHubCredentials.load("github-pat").token.get_secret_value() # dlt.secrets["sources.access_token"]
    os.environ["SOURCES__ACCESS_TOKEN"] = pat
    
def make_bq_destination():
    gcp = GcpCredentials.load("gcp-creds")
    bq  = BigQueryWarehouse.load("bq-warehouse")
    creds = gcp.get_credentials_from_service_account()
    # Prefer the project from your BigQuery block, then fall back to the creds JSON
    project = bq.project or gcp.service_account_info.get("project_id")
    if not project:
        raise ValueError(
            "No GCP project found. Set it in the 'bq-warehouse' block or make sure "
            "your service account JSON contains 'project_id'."
        )
    return dlt.destinations.bigquery(credentials=creds, project_id=project)

@task(log_prints=True)
def run_resource(resource_name: str):
    #set env variables
    set_github_pat_env()
    bq_dest = make_bq_destination()
    import data_pipeline
    # pick just one resource from your dlt source
    source = data_pipeline.github_source.with_resources(resource_name)
    # unique pipeline per resource avoids dlt state clashes
    pipeline = dlt.pipeline(
        pipeline_name=f"rest_api_github__{resource_name}",
        destination=bq_dest,
        dataset_name="prefect_orc_demo",
        progress="log",
    )
    info = pipeline.run(source)
    print(f"{resource_name} -> {info}")
    return info

@flow(log_prints=True)
def main():
    
    a = run_resource("repos")
    b = run_resource("contributors")
    c = run_resource("releases")
    return a, b, c

if __name__ == "__main__":
    main()