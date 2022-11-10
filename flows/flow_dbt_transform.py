import os
from prefect import flow
from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_gcp.credentials import GcpCredentials

@flow
def flow_dbt_transform():
    gcp_credentials = GcpCredentials(service_account_file="./tf-test-365219-e68b028a905b.json")
    target_configs = BigQueryTargetConfigs(
        #schema="schema",
        #project="project",
        credentials=credentials,
    )
    dbt_cli_profile = DbtCliProfile(
        name="webscraper",
        target="dev",
        target_configs=target_configs,
    )

    result = trigger_dbt_cli_command(
        "dbt run",
        overwrite_profiles=True,
        dbt_cli_profile=dbt_cli_profile,
    )

    return result