ducknng bulk transport benchmark
================

- [Machine and software details](#machine-and-software-details)
- [RPC bulk transfer results](#rpc-bulk-transfer-results)
- [Concurrent reader/writer results](#concurrent-readerwriter-results)
- [HTTP ducknng vs Quack ratio](#http-ducknng-vs-quack-ratio)
- [Notes](#notes)

This report is rendered by `make rpc_bulk_compare`. The default workload
uses `DUCKNNG_BULK_ROWS=100000,1000000,10000000` and
`DUCKNNG_BULK_REPETITIONS=5`; set those environment variables when you
want a smoke render instead of the full benchmark. The concurrent slice
uses `DUCKNNG_CONCURRENT_ROWS`, `DUCKNNG_CONCURRENT_ITERATIONS`, and
`DUCKNNG_CONCURRENT_CLIENTS` with defaults derived from the row-transfer
workload.

It measures three things:

1.  `ducknng` bulk row transfer over its RPC/session surface on `http`,
    `tcp`, `ipc`, and `ws`, in both `arrow_ipc_stream` and
    `ducknng_quack_batch` modes.
2.  concurrent ducknng reader, writer, and mixed reader/writer RPC
    workloads across those transports and serializers.
3.  `quack` bulk row transfer over its published client/server surface.

`quack` is only compared on its own public client path. `ducknng` is
compared across multiple transports because transport selection is part
of its SQL-facing contract.

## Machine and software details

| Field                       | Value                                  |
|:----------------------------|:---------------------------------------|
| generated_at                | 2026-05-15 21:28:17 UTC                |
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
| ducknng_git_commit          | e275d6c                                |
| quack_install_source        | INSTALL quack FROM core_nightly        |
| dataset                     | tpch_sf2.lineitem                      |
| lineitem_rows_available     | 11997996                               |
| repetitions                 | 5                                      |
| ducknng_transports          | http,tcp,ipc,ws                        |
| ducknng_serialization_modes | arrow_ipc_stream,ducknng_quack_batch   |
| concurrent_rows             | 100000                                 |
| concurrent_iterations       | 2                                      |
| concurrent_clients          | 2                                      |
| quack_uri                   | quack:localhost:19494                  |

## RPC bulk transfer results

These runs execute `SELECT * FROM lineitem LIMIT n` against the server
side and validate the returned data by aggregate checksum.

| benchmark                    | dataset           | system  | protocol    | transport | serialization_mode  |     rows | repetitions | median_seconds | min_seconds | max_seconds | timings_seconds               |
|:-----------------------------|:------------------|:--------|:------------|:----------|:--------------------|---------:|------------:|---------------:|------------:|------------:|:------------------------------|
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |   100000 |           5 |          0.102 |       0.089 |       0.124 | 0.106,0.124,0.089,0.089,0.102 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |  1000000 |           5 |          0.928 |       0.883 |       0.970 | 0.923,0.928,0.883,0.946,0.970 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    | 10000000 |           5 |          6.654 |       6.337 |       7.162 | 7.016,6.337,6.482,7.162,6.654 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |   100000 |           5 |          0.083 |       0.067 |       0.088 | 0.067,0.083,0.084,0.082,0.088 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |  1000000 |           5 |          0.631 |       0.624 |       0.688 | 0.653,0.688,0.624,0.628,0.631 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch | 10000000 |           5 |          5.380 |       4.921 |       5.623 | 5.380,5.623,5.461,4.933,4.921 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |   100000 |           5 |          0.095 |       0.090 |       0.104 | 0.095,0.104,0.097,0.093,0.090 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |  1000000 |           5 |          0.732 |       0.655 |       0.907 | 0.907,0.762,0.659,0.655,0.732 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  | 10000000 |           5 |          4.189 |       4.120 |       4.235 | 4.235,4.181,4.189,4.120,4.198 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |   100000 |           5 |          0.083 |       0.082 |       0.092 | 0.083,0.085,0.082,0.092,0.083 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |  1000000 |           5 |          0.783 |       0.759 |       0.950 | 0.950,0.813,0.759,0.776,0.783 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    | 10000000 |           5 |          5.894 |       5.891 |       6.127 | 5.894,5.891,5.911,6.127,5.893 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |   100000 |           5 |          0.068 |       0.066 |       0.081 | 0.068,0.081,0.068,0.069,0.066 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |  1000000 |           5 |          0.610 |       0.582 |       0.691 | 0.660,0.610,0.691,0.582,0.609 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch | 10000000 |           5 |          5.074 |       4.497 |       5.147 | 5.147,5.074,5.098,4.691,4.497 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |   100000 |           5 |          0.081 |       0.076 |       0.105 | 0.079,0.081,0.105,0.089,0.076 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |  1000000 |           5 |          0.721 |       0.703 |       0.775 | 0.743,0.711,0.721,0.775,0.703 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    | 10000000 |           5 |          5.442 |       5.226 |       6.018 | 5.442,5.378,6.018,5.496,5.226 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |   100000 |           5 |          0.065 |       0.064 |       0.069 | 0.069,0.065,0.065,0.066,0.064 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |  1000000 |           5 |          0.563 |       0.562 |       0.565 | 0.565,0.563,0.562,0.562,0.565 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch | 10000000 |           5 |          4.317 |       4.145 |       5.087 | 4.571,4.259,5.087,4.145,4.317 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |   100000 |           5 |          0.092 |       0.084 |       0.111 | 0.109,0.111,0.085,0.084,0.092 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |  1000000 |           5 |          0.827 |       0.780 |       0.870 | 0.870,0.856,0.780,0.825,0.827 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    | 10000000 |           5 |          6.431 |       6.267 |       6.649 | 6.649,6.507,6.424,6.267,6.431 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |   100000 |           5 |          0.075 |       0.071 |       0.085 | 0.075,0.071,0.075,0.085,0.077 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |  1000000 |           5 |          0.661 |       0.630 |       0.708 | 0.708,0.705,0.630,0.648,0.661 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch | 10000000 |           5 |          4.855 |       4.561 |       5.135 | 4.561,4.640,4.887,5.135,4.855 |

## Concurrent reader/writer results

These runs open multiple client connections against the same ducknng
service. Reader workers fetch row batches through
`ducknng_query_rpc(...)` / `ducknng_query_rpc_mode(...)`; writer workers
issue `INSERT` statements through `ducknng_run_rpc(...)`. The mixed
scenario runs the configured number of readers and writers at the same
time.

| benchmark             | system  | protocol | transport | serialization_mode  | scenario         | readers | writers | rows_per_reader_operation | iterations_per_worker | wall_seconds | rows_read | write_ops | rows_per_second | write_ops_per_second | median_latency_seconds | p95_latency_seconds |
|:----------------------|:--------|:---------|:----------|:--------------------|:-----------------|--------:|--------:|--------------------------:|----------------------:|-------------:|----------:|----------:|----------------:|---------------------:|-----------------------:|--------------------:|
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.645 |    400000 |         4 |        620155.0 |                  6.2 |                  0.018 |               0.028 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.557 |    400000 |         0 |        718132.9 |                  0.0 |                  0.024 |               0.026 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.527 |         0 |         4 |             0.0 |                  7.6 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.640 |    400000 |         4 |        625000.0 |                  6.2 |                  0.012 |               0.026 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.582 |    400000 |         0 |        687285.2 |                  0.0 |                  0.022 |               0.024 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.542 |         0 |         4 |             0.0 |                  7.4 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.635 |    400000 |         4 |        629921.3 |                  6.3 |                  0.014 |               0.027 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.584 |    400000 |         0 |        684931.5 |                  0.0 |                  0.023 |               0.025 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.521 |         0 |         4 |             0.0 |                  7.7 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.622 |    400000 |         4 |        643086.8 |                  6.4 |                  0.012 |               0.023 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.556 |    400000 |         0 |        719424.5 |                  0.0 |                  0.018 |               0.023 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.528 |         0 |         4 |             0.0 |                  7.6 |                  0.003 |               0.003 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.654 |    400000 |         4 |        611620.8 |                  6.1 |                  0.014 |               0.030 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.585 |    400000 |         0 |        683760.7 |                  0.0 |                  0.023 |               0.028 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.522 |         0 |         4 |             0.0 |                  7.7 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.627 |    400000 |         4 |        637958.5 |                  6.4 |                  0.014 |               0.031 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.588 |    400000 |         0 |        680272.1 |                  0.0 |                  0.022 |               0.023 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.520 |         0 |         4 |             0.0 |                  7.7 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.629 |    400000 |         4 |        635930.0 |                  6.4 |                  0.014 |               0.027 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.607 |    400000 |         0 |        658978.6 |                  0.0 |                  0.026 |               0.030 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.523 |         0 |         4 |             0.0 |                  7.6 |                  0.003 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.633 |    400000 |         4 |        631911.5 |                  6.3 |                  0.013 |               0.025 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.573 |    400000 |         0 |        698080.3 |                  0.0 |                  0.022 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.531 |         0 |         4 |             0.0 |                  7.5 |                  0.002 |               0.002 |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path,
showing both ducknng serializer modes against Quack’s native HTTP path.

|     rows | ducknng_http_arrow_median_seconds | ducknng_http_quack_batch_median_seconds | quack_http_median_seconds | ducknng_arrow_over_quack_ratio | ducknng_quack_batch_over_quack_ratio |
|---------:|----------------------------------:|----------------------------------------:|--------------------------:|-------------------------------:|-------------------------------------:|
|   100000 |                             0.102 |                                   0.083 |                     0.095 |                          1.074 |                                0.874 |
|  1000000 |                             0.928 |                                   0.631 |                     0.732 |                          1.268 |                                0.862 |
| 10000000 |                             6.654 |                                   5.380 |                     4.189 |                          1.588 |                                1.284 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
