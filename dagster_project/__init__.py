from dagster import Definitions, resource, load_assets_from_modules
import os
from . import assets

# COLLEGE FOOTBALL TOKEN
@resource
def get_cfb_token():
    token = os.getenv("CFB_TOKEN")
    return token

# GOOGLE BIG QUERY API TOKEN
@resource
def get_bigquery_token():
    token = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    return token

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "cfb_token": get_cfb_token,
        "bigquery_api_token": get_bigquery_token
    }
)
