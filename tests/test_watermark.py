from unittest.mock import MagicMock, patch

import pytest
from mkpipe.models import ConnectionConfig, ReplicationMethod, TableConfig
from mkpipe_extractor_bigquery import BigQueryExtractor


@pytest.fixture(autouse=True)
def _no_spark_functions():
    """pyspark.sql.functions needs an active session; stub out the ones we use."""
    with patch('pyspark.sql.functions.max', side_effect=lambda c: MagicMock(name=f'max({c})')):
        yield


def _extractor() -> BigQueryExtractor:
    return BigQueryExtractor(
        ConnectionConfig(variant='bigquery', database='proj', schema='ds')
    )


def _table(**kwargs) -> TableConfig:
    defaults = dict(
        name='events',
        target_name='raw__events',
        replication_method=ReplicationMethod.INCREMENTAL,
        iterate_column=['created_at', 'updated_at'],
        iterate_column_type='datetime',
    )
    defaults.update(kwargs)
    return TableConfig(**defaults)


class _FakeAggDf:
    def __init__(self, row):
        self._row = row

    def agg(self, *_exprs):
        m = MagicMock()
        m.first.return_value = self._row
        return m


def _spark(row):
    """spark.read.format(...).option(...) chain -> df with canned agg row."""
    spark = MagicMock()
    reader = MagicMock()
    reader.option.return_value = reader
    spark.read.format.return_value = reader
    df = _FakeAggDf(row)
    reader.load.return_value = df
    return spark, reader


def test_incremental_watermark_and_single_read():
    spark, reader = _spark({'_mk_m0': '2026-01-01', '_mk_m1': '2026-01-02'})
    result = _extractor().extract(_table(), spark, last_point='2025-12-31')
    filter_opt = reader.option.call_args_list[-1].args[1]
    assert '_mk_m0' not in str(filter_opt)
    assert "updated_at >= '2025-12-31'" in filter_opt
    assert result.write_mode == 'append'
    assert result.last_point_value == '2026-01-02'
    assert result.df is not None
    reader.load.assert_called_once()  # data read once; agg runs on same df


def test_incremental_empty_returns_none():
    spark, reader = _spark({'_mk_m0': None, '_mk_m1': None})
    result = _extractor().extract(_table(), spark, last_point='2025-12-31')
    assert result.df is None
    assert result.write_mode == 'append'


def test_empty_first_run_returns_df():
    spark, reader = _spark({'_mk_m0': None, '_mk_m1': None})
    result = _extractor().extract(_table(), spark, last_point=None)
    assert result.df is not None
    assert result.write_mode == 'overwrite'
    assert result.last_point_value is None
