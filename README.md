# mkpipe-extractor-bigquery

Google BigQuery extractor plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Reads BigQuery tables into Spark DataFrames using the `spark-bigquery-connector` via the BigQuery Storage Read API.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  bq_source:
    variant: bigquery
    database: my-gcp-project
    schema: my_dataset
    credentials_file: /path/to/service-account.json
```

---

## Table Configuration

```yaml
pipelines:
  - name: bq_to_pg
    source: bq_source
    destination: pg_target
    tables:
      - name: my_table
        target_name: stg_my_table
        replication_method: full
```

### Incremental Replication

```yaml
      - name: my_table
        target_name: stg_my_table
        replication_method: incremental
        iterate_column: updated_at
        iterate_column_type: datetime
```

---

## Read Parallelism

The BigQuery Storage Read API creates parallel read streams automatically. `partitions_count` sets the `maxParallelism` option, which hints the maximum number of streams BigQuery should create:

```yaml
      - name: my_table
        target_name: stg_my_table
        replication_method: full
        partitions_count: 8     # request up to 8 parallel read streams
```

### How it works

- BigQuery Storage Read API creates server-side read streams; `maxParallelism` is an upper bound hint
- Each stream maps to one Spark partition, read in parallel by Spark executors
- BigQuery may return fewer streams than requested depending on table size

### Performance Notes

- For small tables, BigQuery will naturally use few streams regardless of `maxParallelism`.
- For large tables (>100M rows), setting `partitions_count: 8–32` ensures BigQuery opens enough streams to saturate your Spark cluster.
- The actual parallelism is also limited by the number of available Spark executor cores.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | BigQuery table name |
| `target_name` | string | required | Destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `iterate_column` | string | — | Column used for incremental filter (`>`) |
| `iterate_column_type` | string | — | Type hint for watermark column |
| `partitions_count` | int | — | Max parallel read streams (`maxParallelism`) |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
