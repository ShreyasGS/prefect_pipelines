from prefect import flow, task
from prefect.blocks.system import Secret
import os
import dlt
from prefect_github import GitHubCredentials
from prefect.task_runners import ThreadPoolTaskRunner
from datetime import datetime, timedelta, timezone
from dlt.sources.rest_api import rest_api_source
 

def _set_env(md_db: str):
    # GitHub PAT (GitHubCredentials.token is SecretStr -> use .get_secret_value())
    pat = GitHubCredentials.load("github-pat").token.get_secret_value()
    os.environ["SOURCES__ACCESS_TOKEN"] = pat  # dlt.secrets["sources.access_token"]
    # MotherDuck auth + DB name
    md_token = Secret.load("motherduck-token").get()
    os.environ["MOTHERDUCK_TOKEN"] = md_token
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD"] = md_token
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE"] = md_db

@task(retries=2, log_prints=True)
def run_resource(resource_name: str, md_db: str, start_date: str = None, end_date: str = None):

    _set_env(md_db)
    import example_pipeline,copy
    cfg = copy.deepcopy(example_pipeline.config)

    if resource_name == "forks" and start_date and end_date:
        for res in cfg["resources"]:
            if res["name"] == "forks":
                res["endpoint"]["incremental"]["initial_value"] = start_date
                res["endpoint"]["incremental"]["end_value"] = end_date
    # pick just one resource from your dlt source
    src = rest_api_source(cfg).with_resources(resource_name)
    # unique pipeline per resource avoids dlt state clashes
    pipe = dlt.pipeline(
        pipeline_name=f"rest_api_github__{resource_name}",
        destination="motherduck",
        dataset_name="rest_api_data_mltx",
        progress="log",
    )
    info = pipe.run(src)
    print(f"{resource_name} -> {info}")
    return info

@flow(task_runner=ThreadPoolTaskRunner(max_workers=5), log_prints=True)
def main(md_db: str = "dlt_test"):

    end_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=1)

    start_iso = start_dt.isoformat()  # Outputs: 2025-08-30T00:00:00+00:00
    end_iso = end_dt.isoformat() 
    
    a = run_resource.submit("repos", md_db)
    b = run_resource.submit("contributors", md_db)
    c = run_resource.submit("issues", md_db)
    d = run_resource.submit("forks", md_db, start_date=start_iso, end_date=end_iso)
    e = run_resource.submit("releases", md_db)
    return a.result(), b.result(), c.result(), d.result(), e.result()

if __name__ == "__main__":
    main()
