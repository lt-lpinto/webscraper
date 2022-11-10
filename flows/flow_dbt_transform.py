import os
from prefect import flow
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_gcp.credentials import GcpCredentials

@flow
def flow_dbt_transform(gcp_project, dw_schema, service_account_file):
    gcp_credentials = GcpCredentials(service_account_file=service_account_file)
    target_configs = BigQueryTargetConfigs(
        schema=dw_schema,
        project=gcp_project,
        credentials=gcp_credentials,
    )
    dbt_cli_profile = DbtCliProfile(
        name="webscraper",
        target="webscraper",
        target_configs=target_configs,
    )

    result = trigger_dbt_cli_command(
        "dbt run",
        overwrite_profiles=True,
        dbt_cli_profile=dbt_cli_profile,
    )

    return result