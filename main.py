import os
from prefect import flow, tasks
from .flows.flow_get__allinone import flow_get__allinone
from .flows.flow_get__allinone_popup_links import flow_get__allinone_popup_links
from .flows.flow_dbt_transform import flow_dbt_transform
from dotenv import load_dotenv

load_dotenv(dotenv_path="credentials.env")

SCHEMA  = os.getenv('SCHEMA')
PROJECT = os.getenv('PROJECT')
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')

@flow
def main_flow():
    flow_get__allinone(SERVICE_ACCOUNT_FILE)
    flow_get__allinone_popup_links(SERVICE_ACCOUNT_FILE)
    flow_dbt_transform(PROJECT, SCHEMA, SERVICE_ACCOUNT_FILE)
    
main_flow()