from prefect import flow
from prefect.blocks.system import Secret
import os
from github_pipeline import run_pipeline  # imports your code

@flow(log_prints=True)
def main(db_name: str = "dlt_test"):
    # GitHub PAT for your dlt REST source
    # (dlt.secrets["sources.access_token"] -> env var SOURCES__ACCESS_TOKEN)
    os.environ["SOURCES__ACCESS_TOKEN"] = Secret.load("github-pat").value

    # MotherDuck token from your block
    md_token = Secret.load("motherduck-token").value
    
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD"] = md_token

    # Tell dlt which MotherDuck database to use
    os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE"] = db_name

    return run_pipeline()

if __name__ == "__main__":
    main()
