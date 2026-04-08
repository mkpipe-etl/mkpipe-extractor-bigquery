from typing import Optional

from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.spark.base import BaseExtractor
from mkpipe.utils import get_logger

JAR_PACKAGES = ['com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.1']

logger = get_logger(__name__)


class BigQueryExtractor(BaseExtractor, variant='bigquery'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.project = connection.database
        self.dataset = connection.schema
        self.credentials_file = connection.credentials_file
        self._billing_project_id = (
            connection.extra.get('billing_project') if connection.extra else None
        )

    def _billing_project(self) -> str:
        """Resolve the billing/quota project for BigQuery API calls.

        Priority: connection.extra.billing_project > credentials JSON project_id > self.project
        """
        if self._billing_project_id:
            return self._billing_project_id

        # Try to read project_id from service account JSON
        if self.credentials_file:
            import json
            from pathlib import Path

            creds_path = Path(self.credentials_file)
            if creds_path.exists():
                try:
                    with open(creds_path) as f:
                        creds = json.load(f)
                    project_id = creds.get('project_id')
                    if project_id:
                        return project_id
                except Exception:
                    pass

        return self.project

    def extract(
        self, table: TableConfig, spark, last_point: Optional[str] = None
    ) -> ExtractResult:
        logger.info(
            {
                'table': table.target_name,
                'status': 'extracting',
                'replication_method': table.replication_method.value,
            }
        )

        reader = (
            spark.read.format('bigquery')
            .option('project', self.project)
            .option('dataset', self.dataset)
            .option('table', table.name)
        )

        if self.credentials_file:
            reader = reader.option('credentialsFile', self.credentials_file)
            # When reading from a different project (e.g. public datasets),
            # parentProject must point to the billing project from credentials.
            reader = reader.option('parentProject', self._billing_project())

        if table.partitions_count:
            reader = reader.option('maxParallelism', str(table.partitions_count))

        if (
            table.replication_method.value == 'incremental'
            and last_point
            and table.iterate_column
        ):
            # BigQuery is strict about type matching: INT64 > STRING will fail.
            # Use iterate_column_type to decide whether to quote the value.
            if table.iterate_column_type == 'int':
                filter_expr = f"{table.iterate_column} > {last_point}"
            else:
                filter_expr = f"{table.iterate_column} > '{last_point}'"
            reader = reader.option('filter', filter_expr)
            write_mode = 'append'
        else:
            write_mode = 'overwrite'

        df = reader.load()

        last_point_value = None
        if table.replication_method.value == 'incremental' and table.iterate_column:
            from pyspark.sql import functions as F

            row = df.agg(F.max(table.iterate_column).alias('max_val')).first()
            if row and row['max_val'] is not None:
                last_point_value = str(row['max_val'])

        logger.info(
            {
                'table': table.target_name,
                'status': 'extracted',
                'write_mode': write_mode,
            }
        )

        return ExtractResult(
            df=df, write_mode=write_mode, last_point_value=last_point_value
        )
