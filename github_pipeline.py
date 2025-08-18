import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": dlt.secrets["sources.access_token"], 
        },
        "paginator": "header_link" 
    },
    "resources": [  
        {
            "name": "repos",
            "endpoint": {
                "path": "orgs/dlt-hub/repos"
            },
        },
    ],
}

github_source = rest_api_source(config)

def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="motherduck",
        dataset_name="rest_api_data",
        progress="log",
    )

    load_info = pipeline.run(github_source)
    print(load_info)
