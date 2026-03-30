from typing import Optional

from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.spark.base import BaseExtractor
from mkpipe.utils import get_logger

JAR_PACKAGES = ['com.google.cloud.spark:spark-4.1-bigquery:0.44.1-preview']

logger = get_logger(__name__)


class BigQueryExtractor(BaseExtractor, variant='bigquery'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.project = connection.database
        self.dataset = connection.schema
        self.credentials_file = connection.credentials_file

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

        if table.partitions_count:
            reader = reader.option('maxParallelism', str(table.partitions_count))

        if (
            table.replication_method.value == 'incremental'
            and last_point
            and table.iterate_column
        ):
            reader = reader.option('filter', f"{table.iterate_column} > '{last_point}'")
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
