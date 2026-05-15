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
| generated_at                | 2026-05-15 22:08:28 UTC                |
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
| ducknng_git_commit          | 06a3e09                                |
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
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |   100000 |           5 |          0.104 |       0.096 |       0.131 | 0.104,0.104,0.098,0.131,0.096 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    |  1000000 |           5 |          0.823 |       0.776 |       0.851 | 0.807,0.824,0.823,0.851,0.776 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | arrow_ipc_stream    | 10000000 |           5 |          6.904 |       6.400 |       7.199 | 6.904,7.199,6.568,6.400,6.904 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |   100000 |           5 |          0.075 |       0.062 |       0.078 | 0.062,0.075,0.076,0.069,0.078 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch |  1000000 |           5 |          0.636 |       0.549 |       0.663 | 0.663,0.644,0.550,0.636,0.549 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | ducknng_quack_batch | 10000000 |           5 |          5.759 |       4.442 |       6.179 | 6.041,5.759,6.179,4.442,5.596 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |   100000 |           5 |          0.104 |       0.089 |       0.112 | 0.098,0.089,0.105,0.104,0.112 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  |  1000000 |           5 |          0.633 |       0.609 |       0.953 | 0.953,0.747,0.633,0.617,0.609 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | application/duckdb  | 10000000 |           5 |          4.181 |       4.170 |       4.278 | 4.253,4.278,4.181,4.170,4.180 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |   100000 |           5 |          0.080 |       0.078 |       0.083 | 0.083,0.081,0.080,0.079,0.078 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    |  1000000 |           5 |          0.784 |       0.738 |       0.880 | 0.880,0.784,0.738,0.798,0.765 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | arrow_ipc_stream    | 10000000 |           5 |          6.176 |       5.658 |       6.313 | 6.313,6.176,6.217,5.658,6.163 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |   100000 |           5 |          0.059 |       0.059 |       0.063 | 0.059,0.059,0.059,0.063,0.060 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch |  1000000 |           5 |          0.654 |       0.554 |       0.811 | 0.634,0.654,0.811,0.735,0.554 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | ducknng_quack_batch | 10000000 |           5 |          4.230 |       3.787 |       5.624 | 5.624,3.787,4.230,4.935,4.132 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |   100000 |           5 |          0.077 |       0.076 |       0.088 | 0.088,0.082,0.077,0.076,0.076 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    |  1000000 |           5 |          0.736 |       0.698 |       0.795 | 0.736,0.698,0.717,0.795,0.750 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | arrow_ipc_stream    | 10000000 |           5 |          5.408 |       5.358 |       5.671 | 5.408,5.446,5.671,5.382,5.358 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |   100000 |           5 |          0.062 |       0.060 |       0.069 | 0.069,0.062,0.066,0.062,0.060 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch |  1000000 |           5 |          0.613 |       0.544 |       0.639 | 0.544,0.619,0.608,0.639,0.613 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | ducknng_quack_batch | 10000000 |           5 |          3.876 |       3.544 |       4.032 | 3.876,4.032,3.959,3.544,3.811 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |   100000 |           5 |          0.091 |       0.084 |       0.095 | 0.091,0.095,0.084,0.093,0.084 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    |  1000000 |           5 |          0.744 |       0.736 |       0.865 | 0.865,0.767,0.744,0.736,0.741 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | arrow_ipc_stream    | 10000000 |           5 |          6.127 |       6.018 |       6.603 | 6.603,6.127,6.104,6.018,6.203 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |   100000 |           5 |          0.074 |       0.069 |       0.088 | 0.074,0.069,0.070,0.086,0.088 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch |  1000000 |           5 |          0.601 |       0.558 |       0.728 | 0.728,0.599,0.558,0.680,0.601 |
| bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | ducknng_quack_batch | 10000000 |           5 |          4.871 |       3.897 |       5.428 | 3.897,4.220,4.871,5.428,5.122 |

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
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.634 |             0.058 |    400000 |       400000 |         4 |         6896552 |               6896552 |                 69.0 |                  0.015 |               0.031 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.588 |             0.056 |    400000 |            0 |         0 |         7142857 |                     0 |                  0.0 |                  0.028 |               0.031 |
| concurrent_read_write | ducknng | rpc      | http      | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.544 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.615 |             0.042 |    400000 |       400000 |         4 |         9523810 |               9523810 |                 95.2 |                  0.011 |               0.026 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.579 |             0.045 |    400000 |            0 |         0 |         8888889 |                     0 |                  0.0 |                  0.022 |               0.023 |
| concurrent_read_write | ducknng | rpc      | http      | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.534 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.002 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.638 |             0.050 |    400000 |       400000 |         4 |         8000000 |               8000000 |                 80.0 |                  0.015 |               0.026 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.584 |             0.047 |    400000 |            0 |         0 |         8510638 |                     0 |                  0.0 |                  0.022 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ipc       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.529 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.617 |             0.046 |    400000 |       400000 |         4 |         8695652 |               8695652 |                 87.0 |                  0.014 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.587 |             0.054 |    400000 |            0 |         0 |         7407407 |                     0 |                  0.0 |                  0.026 |               0.030 |
| concurrent_read_write | ducknng | rpc      | ipc       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.540 |             0.004 |         0 |       400000 |         4 |               0 |             100000000 |               1000.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.620 |             0.056 |    400000 |       400000 |         4 |         7142857 |               7142857 |                 71.4 |                  0.019 |               0.028 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.577 |             0.054 |    400000 |            0 |         0 |         7407407 |                     0 |                  0.0 |                  0.027 |               0.029 |
| concurrent_read_write | ducknng | rpc      | tcp       | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.540 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.630 |             0.053 |    400000 |       400000 |         4 |         7547170 |               7547170 |                 75.5 |                  0.019 |               0.030 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.578 |             0.053 |    400000 |            0 |         0 |         7547170 |                     0 |                  0.0 |                  0.026 |               0.029 |
| concurrent_read_write | ducknng | rpc      | tcp       | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.530 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.633 |             0.056 |    400000 |       400000 |         4 |         7142857 |               7142857 |                 71.4 |                  0.014 |               0.030 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | readers          |       2 |       0 |                    100000 |                     2 |        0.593 |             0.045 |    400000 |            0 |         0 |         8888889 |                     0 |                  0.0 |                  0.022 |               0.024 |
| concurrent_read_write | ducknng | rpc      | ws        | arrow_ipc_stream    | writers          |       0 |       2 |                    100000 |                     2 |        0.523 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | mixed_read_write |       2 |       2 |                    100000 |                     2 |        0.646 |             0.062 |    400000 |       400000 |         4 |         6451613 |               6451613 |                 64.5 |                  0.017 |               0.036 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | readers          |       2 |       0 |                    100000 |                     2 |        0.591 |             0.046 |    400000 |            0 |         0 |         8695652 |                     0 |                  0.0 |                  0.022 |               0.023 |
| concurrent_read_write | ducknng | rpc      | ws        | ducknng_quack_batch | writers          |       0 |       2 |                    100000 |                     2 |        0.546 |             0.005 |         0 |       400000 |         4 |               0 |              80000000 |                800.0 |                  0.002 |               0.003 |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path,
showing both ducknng serializer modes against Quack’s native HTTP path.

|     rows | ducknng_http_arrow_median_seconds | ducknng_http_quack_batch_median_seconds | quack_http_median_seconds | ducknng_arrow_over_quack_ratio | ducknng_quack_batch_over_quack_ratio |
|---------:|----------------------------------:|----------------------------------------:|--------------------------:|-------------------------------:|-------------------------------------:|
|   100000 |                             0.104 |                                   0.075 |                     0.104 |                          1.000 |                                0.721 |
|  1000000 |                             0.823 |                                   0.636 |                     0.633 |                          1.300 |                                1.005 |
| 10000000 |                             6.904 |                                   5.759 |                     4.181 |                          1.651 |                                1.377 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
