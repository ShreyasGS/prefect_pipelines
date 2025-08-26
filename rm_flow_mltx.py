from prefect import flow, task
from prefect.blocks.system import Secret
import os
import dlt
from prefect_github import GitHubCredentials
from prefect.task_runners import ThreadPoolTaskRunner
 

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
def run_resource(resource_name: str, md_db: str):
    _set_env(md_db)
    import github_mlt_pipeline
    # pick just one resource from your dlt source
    src = github_mlt_pipeline.github_source.with_resources(resource_name)
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

@flow(task_runner=ThreadPoolTaskRunner(max_workers=2), log_prints=True)
def main(md_db: str = "dlt_test"):
    
    a = run_resource.submit("repos", md_db)
    b = run_resource.submit("contributors", md_db)
    c = run_resource("issues", md_db)
    return a.result(), b.result(), c.result()

if __name__ == "__main__":
    main()
