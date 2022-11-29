import json
import os

import pytest

from dbt.tests.adapter.basic.test_empty import BaseEmpty

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local


pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    credentials_json_str = os.getenv("BIGQUERY_TEST_SERVICE_ACCOUNT_JSON").replace("'", '"')
    credentials = json.loads(credentials_json_str)
    project_id = credentials.get("project_id")

    return {
        "type": "infer",
        "database": os.getenv("INFER_URL"),
        "apikey": os.getenv("INFER_KEY"),
        "schema": os.getenv("INFER_USER"),
        "data_config": {
            "type": "bigquery",
            "keyfile_json": credentials,
            "method": "service-account-json",
            "project": project_id,
            "threads": 1,
            "schema": os.getenv("BIGQUERY_SCHEMA"),
        },
    }
