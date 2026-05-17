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
| generated_at                | 2026-05-17 14:54:10 UTC                |
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
| ducknng_git_commit          | 673c7d7                                |
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
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |   100000 |           5 |          0.104 |       0.084 |       0.108 | 0.107,0.104,0.085,0.084,0.108 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |  1000000 |           5 |          0.841 |       0.764 |       0.877 | 0.853,0.821,0.877,0.764,0.841 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    | 10000000 |           5 |          6.687 |       6.494 |       6.781 | 6.534,6.494,6.687,6.781,6.732 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |   100000 |           5 |          0.063 |       0.059 |       0.071 | 0.062,0.063,0.071,0.059,0.070 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |  1000000 |           5 |          0.717 |       0.654 |       0.873 | 0.873,0.717,0.708,0.764,0.654 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch | 10000000 |           5 |          5.643 |       4.534 |       6.003 | 6.003,4.534,5.478,5.991,5.643 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |   100000 |           5 |          0.099 |       0.083 |       0.114 | 0.083,0.114,0.100,0.099,0.097 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |  1000000 |           5 |          0.649 |       0.628 |       0.843 | 0.843,0.691,0.649,0.628,0.639 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  | 10000000 |           5 |          4.249 |       4.210 |       4.347 | 4.249,4.210,4.219,4.320,4.347 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |   100000 |           5 |          0.078 |       0.077 |       0.079 | 0.079,0.078,0.079,0.078,0.077 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |  1000000 |           5 |          0.685 |       0.666 |       0.743 | 0.743,0.685,0.666,0.672,0.686 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    | 10000000 |           5 |          5.168 |       5.103 |       5.592 | 5.168,5.535,5.103,5.115,5.592 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |   100000 |           5 |          0.058 |       0.057 |       0.059 | 0.058,0.059,0.058,0.058,0.057 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |  1000000 |           5 |          0.702 |       0.581 |       0.740 | 0.740,0.721,0.670,0.581,0.702 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch | 10000000 |           5 |          4.455 |       3.942 |       5.089 | 5.089,3.942,4.246,4.455,4.967 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |   100000 |           5 |          0.082 |       0.074 |       0.099 | 0.076,0.082,0.085,0.099,0.074 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |  1000000 |           5 |          0.645 |       0.645 |       0.680 | 0.680,0.645,0.645,0.645,0.654 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    | 10000000 |           5 |          5.219 |       4.801 |       5.591 | 5.341,5.219,5.591,5.002,4.801 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |   100000 |           5 |          0.055 |       0.054 |       0.057 | 0.054,0.057,0.056,0.055,0.055 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |  1000000 |           5 |          0.509 |       0.502 |       0.672 | 0.672,0.502,0.503,0.635,0.509 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch | 10000000 |           5 |          3.682 |       3.121 |       4.528 | 3.422,4.528,3.908,3.682,3.121 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |   100000 |           5 |          0.080 |       0.079 |       0.084 | 0.084,0.082,0.080,0.079,0.079 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |  1000000 |           5 |          0.723 |       0.706 |       0.796 | 0.740,0.712,0.723,0.706,0.796 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    | 10000000 |           5 |          6.324 |       6.049 |       6.496 | 6.065,6.324,6.049,6.496,6.357 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |   100000 |           5 |          0.068 |       0.064 |       0.072 | 0.072,0.072,0.066,0.068,0.064 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |  1000000 |           5 |          0.672 |       0.570 |       0.716 | 0.630,0.716,0.672,0.570,0.685 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch | 10000000 |           5 |          5.579 |       4.455 |       6.176 | 5.579,5.611,4.455,5.325,6.176 |

## Concurrent reader/writer results

These runs open multiple client connections against the same ducknng
service. Reader workers fetch row batches through
`ducknng_query_rpc(...)` / `ducknng_query_rpc_mode(...)`; writer workers
issue set-oriented `INSERT ... SELECT FROM range(...)` statements
through `ducknng_run_rpc(...)`. The mixed scenario runs the configured
number of readers and writers at the same time. `wall_seconds` includes
benchmark orchestration around the worker calls; throughput columns use
`operation_seconds`, the maximum measured in-worker operation time, so
writer startup and extension-load overhead do not dominate the
write-rate numbers.

| benchmark             | system  | protocol | transport | serialization_mode  | scenario         | readers | writers | rows_per_reader_operation | iterations_per_worker | wall_seconds | operation_seconds | rows_read | rows_written | write_ops | rows_per_second | write_rows_per_second | write_ops_per_second | median_latency_seconds | p95_latency_seconds |
|:----------------------|:--------|:---------|:----------|:--------------------|:-----------------|--------:|--------:|--------------------------:|----------------------:|-------------:|------------------:|----------:|-------------:|----------:|----------------:|----------------------:|---------------------:|-----------------------:|--------------------:|
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.642 |             0.059 |    400000 |       400000 |         4 |         6779661 |               6779661 |                 67.8 |                  0.015 |               0.030 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.570 |             0.048 |    400000 |            0 |         0 |         8333333 |                     0 |                  0.0 |                  0.024 |               0.024 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.548 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.623 |             0.055 |    400000 |       400000 |         4 |         7272727 |               7272727 |                 72.7 |                  0.013 |               0.030 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.581 |             0.044 |    400000 |            0 |         0 |         9090909 |                     0 |                  0.0 |                  0.022 |               0.023 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.528 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.626 |             0.057 |    400000 |       400000 |         4 |         7017544 |               7017544 |                 70.2 |                  0.015 |               0.031 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.600 |             0.053 |    400000 |            0 |         0 |         7547170 |                     0 |                  0.0 |                  0.025 |               0.030 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.519 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.620 |             0.035 |    400000 |       400000 |         4 |        11428571 |              11428571 |                114.3 |                  0.014 |               0.022 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.596 |             0.048 |    400000 |            0 |         0 |         8333333 |                     0 |                  0.0 |                  0.024 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.535 |             0.003 |         0 |       400000 |         4 |               0 |             133333333 |               1333.3 |                  0.001 |               0.002 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.630 |             0.054 |    400000 |       400000 |         4 |         7407407 |               7407407 |                 74.1 |                  0.015 |               0.032 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.559 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.023 |               0.024 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.519 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.633 |             0.054 |    400000 |       400000 |         4 |         7407407 |               7407407 |                 74.1 |                  0.013 |               0.032 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.562 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.023 |               0.024 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.523 |             0.003 |         0 |       400000 |         4 |               0 |             133333333 |               1333.3 |                  0.001 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.631 |             0.050 |    400000 |       400000 |         4 |         8000000 |               8000000 |                 80.0 |                  0.017 |               0.026 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.608 |             0.048 |    400000 |            0 |         0 |         8333333 |                     0 |                  0.0 |                  0.024 |               0.025 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.536 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.636 |             0.056 |    400000 |       400000 |         4 |         7142857 |               7142857 |                 71.4 |                  0.015 |               0.032 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.598 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.023 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.528 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path,
showing both ducknng serializer modes against Quack’s native HTTP path.

|     rows | ducknng_http_arrow_median_seconds | ducknng_http_quack_batch_median_seconds | quack_http_median_seconds | ducknng_arrow_over_quack_ratio | ducknng_quack_batch_over_quack_ratio |
|---------:|----------------------------------:|----------------------------------------:|--------------------------:|-------------------------------:|-------------------------------------:|
|   100000 |                             0.104 |                                   0.063 |                     0.099 |                          1.051 |                                0.636 |
|  1000000 |                             0.841 |                                   0.717 |                     0.649 |                          1.296 |                                1.105 |
| 10000000 |                             6.687 |                                   5.643 |                     4.249 |                          1.574 |                                1.328 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
