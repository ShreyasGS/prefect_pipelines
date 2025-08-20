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
        {
          "name": "contributors",
          "endpoint": {
            "path": "repos/dlt-hub/dlt/contributors",
          },
        },
        {
          "name": "issues",
          "endpoint": {
            "path": "repos/dlt-hub/dlt/issues",
          },
        },
    ],
}

github_source = rest_api_source(config)
