from prefect import flow
from prefect.blocks.system import Secret
from prefect_github import GitHubCredentials
import os

@flow(log_prints=True)
def main(db_name: str = "dlt_test"):
    # MotherDuck token (Prefect Secret -> str)
    md_token = Secret.load("motherduck-token").get()          # <- use .get(), not .value
    os.environ["MOTHERDUCK_TOKEN"] = md_token
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD"] = md_token
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE"] = db_name

    # GitHub PAT (GitHubCredentials.token is SecretStr -> use .get_secret_value())
    pat = GitHubCredentials.load("github-pat").token.get_secret_value()
    os.environ["SOURCES__ACCESS_TOKEN"] = pat  # dlt.secrets["sources.access_token"]

    import github_pipeline
    return github_pipeline.run_pipeline()


if __name__ == "__main__":
    main()
