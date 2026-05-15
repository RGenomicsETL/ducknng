ducknng bulk transport benchmark
================

- [Machine and software details](#machine-and-software-details)
- [RPC bulk transfer results](#rpc-bulk-transfer-results)
- [HTTP ducknng vs Quack ratio](#http-ducknng-vs-quack-ratio)
- [Notes](#notes)

This report is rendered by `make rpc_bulk_compare`. The default workload
uses `DUCKNNG_BULK_ROWS=100000,1000000,10000000` and
`DUCKNNG_BULK_REPETITIONS=5`; set those environment variables when you
want a smoke render instead of the full benchmark.

It measures two things:

1.  `ducknng` bulk row transfer over its RPC/session surface on `http`,
    `tcp`, `ipc`, and `ws`, in both `arrow_ipc_stream` and
    `quack_batch_v1` modes.
2.  `quack` bulk row transfer over its published client/server surface.

`quack` is only compared on its own public client path. `ducknng` is
compared across multiple transports because transport selection is part
of its SQL-facing contract.

## Machine and software details

| Field                       | Value                                  |
|:----------------------------|:---------------------------------------|
| generated_at                | 2026-05-15 19:54:29 UTC                |
| hostname                    | Ubuntu-2404-noble-amd64-base           |
| sysname                     | Linux                                  |
| release                     | 6.8.0-78-generic                       |
| machine                     | x86_64                                 |
| cpu_model                   | 13th Gen Intel(R) Core(TM) i5-13500    |
| logical_cores               | 20                                     |
| physical_cores              | 20                                     |
| memory_total                | 62.6 GiB                               |
| r_version                   | R version 4.6.0 (2026-04-24)           |
| duckdb_version              | 1.5.2                                  |
| ducknng_extension           | build/release/ducknng.duckdb_extension |
| ducknng_git_commit          | d403851                                |
| quack_install_source        | INSTALL quack FROM core_nightly        |
| dataset                     | tpch_sf1.lineitem                      |
| lineitem_rows_available     | 6001215                                |
| repetitions                 | 1                                      |
| ducknng_transports          | http,tcp,ipc,ws                        |
| ducknng_serialization_modes | arrow_ipc_stream,quack_batch_v1        |
| quack_uri                   | quack:localhost:19494                  |

## RPC bulk transfer results

These runs execute `SELECT * FROM lineitem LIMIT n` against the server
side and validate the returned data by aggregate checksum.

| benchmark                    | dataset           | system  | protocol    | transport | serialization_mode | rows | repetitions | median_seconds | min_seconds | max_seconds | timings_seconds |
|:-----------------------------|:------------------|:--------|:------------|:----------|:-------------------|-----:|------------:|---------------:|------------:|------------:|:----------------|
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | http      | arrow_ipc_stream   | 1000 |           1 |          0.008 |       0.008 |       0.008 | 0.008           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | http      | quack_batch_v1     | 1000 |           1 |          0.010 |       0.010 |       0.010 | 0.010           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | quack   | quack_query | http      | application/duckdb | 1000 |           1 |          0.011 |       0.011 |       0.011 | 0.011           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream   | 1000 |           1 |          0.007 |       0.007 |       0.007 | 0.007           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | ipc       | quack_batch_v1     | 1000 |           1 |          0.007 |       0.007 |       0.007 | 0.007           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream   | 1000 |           1 |          0.008 |       0.008 |       0.008 | 0.008           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | tcp       | quack_batch_v1     | 1000 |           1 |          0.007 |       0.007 |       0.007 | 0.007           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream   | 1000 |           1 |          0.009 |       0.009 |       0.009 | 0.009           |
| bulk_transfer_lineitem_limit | tpch_sf1.lineitem | ducknng | rpc         | ws        | quack_batch_v1     | 1000 |           1 |          0.008 |       0.008 |       0.008 | 0.008           |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path,
showing both ducknng serializer modes against Quack’s native HTTP path.

| rows | ducknng_http_arrow_median_seconds | ducknng_http_quack_batch_median_seconds | quack_http_median_seconds | ducknng_arrow_over_quack_ratio | ducknng_quack_batch_over_quack_ratio |
|-----:|----------------------------------:|----------------------------------------:|--------------------------:|-------------------------------:|-------------------------------------:|
| 1000 |                             0.008 |                                    0.01 |                     0.011 |                          0.727 |                                0.909 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
