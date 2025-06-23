from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data: dict, **kwargs) -> None:
    """
    Exports each DataFrame in `data` to its own table:
      my-uber-date-analytics-project.uber_data_engineering.<dict_key>
    """
    project_id = 'my-uber-date-analytics-project'
    dataset = 'uber_data_engineering'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    client = BigQuery.with_config(
        ConfigFileLoader(config_path, 'default')
    )

    for table_name, df in data.items():
        table_id = f'{project_id}.{dataset}.{table_name}'
        print(f'→ exporting {table_name} to {table_id}')
        client.export(
            df,
            table_id,
            if_exists='replace',
        )
