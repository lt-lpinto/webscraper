from prefect import flow, tasks
from .flows.flow_get__allinone import flow_get__allinone
from .flows.flow_get__allinone_popup_links import flow_get__allinone_popup_links

@flow
def main_flow():
    flow_get__allinone()
    flow_get__allinone_popup_links()
    #flow_get_hotels(api_token, snowflake_connection)
   
    #flow_dbt_transform(snowflake_connection)
    
main_flow()