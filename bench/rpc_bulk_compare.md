ducknng bulk transport benchmark
================

- [Machine and software details](#machine-and-software-details)
- [RPC bulk transfer results](#rpc-bulk-transfer-results)
- [HTTP ducknng vs Quack ratio](#http-ducknng-vs-quack-ratio)
- [Raw socket transport-only echo](#raw-socket-transport-only-echo)
- [Notes](#notes)

This report is rendered by `make rpc_bulk_compare`.

It measures three things:

1.  `ducknng` bulk row transfer over its RPC/session surface on `http`,
    `tcp`, `ipc`, and `ws`.
2.  `quack` bulk row transfer over its published client/server surface.
3.  `ducknng` raw socket round-trip echo over `ipc`, `tcp`, and `ws`,
    using pair sockets without RPC, SQL session, or Arrow row framing in
    the transport measurement itself.

`quack` is only compared on its own public client path. `ducknng` is
compared across multiple transports because transport selection is part
of its SQL-facing contract.

## Machine and software details

``` r
knitr::kable(metadata_long, col.names = c("Field", "Value"))
```

|                         | Field                   | Value                                                |
|:------------------------|:------------------------|:-----------------------------------------------------|
| generated_at            | generated_at            | 2026-05-13 12:44:14 UTC                              |
| hostname                | hostname                | Ubuntu-2404-noble-amd64-base                         |
| sysname                 | sysname                 | Linux                                                |
| release                 | release                 | 6.8.0-78-generic                                     |
| machine                 | machine                 | x86_64                                               |
| cpu_model               | cpu_model               | 13th Gen Intel(R) Core(TM) i5-13500                  |
| logical_cores           | logical_cores           | 20                                                   |
| physical_cores          | physical_cores          | 20                                                   |
| memory_total            | memory_total            | 62.6 GiB                                             |
| r_version               | r_version               | R version 4.6.0 (2026-04-24)                         |
| duckdb_version          | duckdb_version          | 1.5.2                                                |
| ducknng_extension       | ducknng_extension       | /root/ducknng/build/release/ducknng.duckdb_extension |
| ducknng_git_commit      | ducknng_git_commit      | b4a95e7                                              |
| quack_install_source    | quack_install_source    | INSTALL quack FROM core_nightly                      |
| dataset                 | dataset                 | tpch_sf2.lineitem                                    |
| lineitem_rows_available | lineitem_rows_available | 11997996                                             |
| repetitions             | repetitions             | 5                                                    |
| raw_payload_bytes       | raw_payload_bytes       | 1048576,4194304,16777216                             |
| ducknng_transports      | ducknng_transports      | http,tcp,ipc,ws                                      |
| raw_transports          | raw_transports          | ipc,tcp,ws                                           |
| quack_uri               | quack_uri               | quack:localhost:19494                                |

## RPC bulk transfer results

These runs execute `SELECT * FROM lineitem LIMIT n` against the server
side and validate the returned data by aggregate checksum.

``` r
knitr::kable(rpc_results)
```

|     | benchmark                    | dataset           | system  | protocol    | transport |  rows | repetitions | median_seconds | min_seconds | max_seconds | timings_seconds                    |
|:----|:-----------------------------|:------------------|:--------|:------------|:----------|------:|------------:|---------------:|------------:|------------:|:-----------------------------------|
| 1   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | 1e+05 |           5 |          0.159 |       0.132 |       0.176 | 0.173,0.133,0.176,0.132,0.159      |
| 2   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | 1e+06 |           5 |          2.052 |       1.659 |       2.271 | 2.271,1.659,2.111,2.052,1.991      |
| 3   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | http      | 1e+07 |           5 |         13.801 |      12.975 |      15.450 | 13.801,13.353,12.975,15.450,14.279 |
| 13  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | 1e+05 |           5 |          0.092 |       0.088 |       0.118 | 0.091,0.088,0.092,0.118,0.102      |
| 14  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | 1e+06 |           5 |          0.669 |       0.639 |       0.920 | 0.920,0.829,0.669,0.639,0.659      |
| 15  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | quack   | quack_query | http      | 1e+07 |           5 |          4.406 |       4.308 |       4.468 | 4.398,4.308,4.422,4.406,4.468      |
| 7   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | 1e+05 |           5 |          0.091 |       0.086 |       0.098 | 0.091,0.089,0.086,0.097,0.098      |
| 8   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | 1e+06 |           5 |          1.084 |       0.952 |       1.450 | 0.952,1.067,1.147,1.084,1.450      |
| 9   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ipc       | 1e+07 |           5 |          7.387 |       7.030 |       7.622 | 7.387,7.530,7.030,7.622,7.204      |
| 4   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | 1e+05 |           5 |          0.122 |       0.091 |       0.159 | 0.159,0.108,0.091,0.151,0.122      |
| 5   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | 1e+06 |           5 |          0.989 |       0.810 |       1.223 | 0.810,1.081,1.223,0.989,0.908      |
| 6   | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | tcp       | 1e+07 |           5 |          9.725 |       8.041 |      10.616 | 8.041,8.401,9.725,9.754,10.616     |
| 10  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | 1e+05 |           5 |          0.143 |       0.100 |       0.185 | 0.185,0.100,0.122,0.156,0.143      |
| 11  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | 1e+06 |           5 |          1.535 |       1.077 |       1.612 | 1.077,1.535,1.581,1.612,1.225      |
| 12  | bulk_transfer_lineitem_limit | tpch_sf2.lineitem | ducknng | rpc         | ws        | 1e+07 |           5 |         12.686 |      12.426 |      13.220 | 13.019,12.686,12.517,13.220,12.426 |

## HTTP ducknng vs Quack ratio

This is the apples-to-apples comparison on the common HTTP-facing path.

``` r
knitr::kable(ratio_results)
```

|  rows | ducknng_http_median_seconds | quack_http_median_seconds | ducknng_over_quack_ratio |
|------:|----------------------------:|--------------------------:|-------------------------:|
| 1e+05 |                       0.159 |                     0.092 |                    1.728 |
| 1e+06 |                       2.052 |                     0.669 |                    3.067 |
| 1e+07 |                      13.801 |                     4.406 |                    3.132 |

## Raw socket transport-only echo

These runs use `ducknng` pair sockets directly. The client sends a
payload, the server echoes it back, and the client verifies the returned
byte count. This isolates transport-visible round-trip cost without the
RPC envelope or Arrow row encoding.

``` r
knitr::kable(raw_results)
```

| benchmark       | system  | protocol      | transport | payload_bytes | repetitions | median_seconds | min_seconds | max_seconds | median_roundtrip_mib_per_sec | timings_seconds               |
|:----------------|:--------|:--------------|:----------|--------------:|------------:|---------------:|------------:|------------:|-----------------------------:|:------------------------------|
| raw_socket_echo | ducknng | raw_pair_echo | ipc       |       1048576 |           5 |          0.032 |       0.031 |       0.034 |                       62.500 | 0.032,0.032,0.034,0.031,0.032 |
| raw_socket_echo | ducknng | raw_pair_echo | ipc       |       4194304 |           5 |          0.124 |       0.117 |       0.136 |                       64.516 | 0.118,0.124,0.136,0.135,0.117 |
| raw_socket_echo | ducknng | raw_pair_echo | ipc       |      16777216 |           5 |          0.511 |       0.501 |       0.619 |                       62.622 | 0.510,0.562,0.501,0.619,0.511 |
| raw_socket_echo | ducknng | raw_pair_echo | tcp       |       1048576 |           5 |          0.032 |       0.032 |       0.035 |                       62.500 | 0.035,0.032,0.032,0.032,0.032 |
| raw_socket_echo | ducknng | raw_pair_echo | tcp       |       4194304 |           5 |          0.119 |       0.117 |       0.166 |                       67.227 | 0.122,0.117,0.119,0.166,0.117 |
| raw_socket_echo | ducknng | raw_pair_echo | tcp       |      16777216 |           5 |          0.507 |       0.501 |       0.620 |                       63.116 | 0.506,0.619,0.507,0.620,0.501 |
| raw_socket_echo | ducknng | raw_pair_echo | ws        |       1048576 |           5 |          0.033 |       0.032 |       0.033 |                       60.606 | 0.033,0.033,0.033,0.032,0.033 |
| raw_socket_echo | ducknng | raw_pair_echo | ws        |       4194304 |           5 |          0.137 |       0.125 |       0.154 |                       58.394 | 0.125,0.154,0.126,0.137,0.142 |
| raw_socket_echo | ducknng | raw_pair_echo | ws        |      16777216 |           5 |          0.533 |       0.514 |       0.630 |                       60.038 | 0.533,0.619,0.518,0.630,0.514 |

## Notes

- The row benchmark uses a local TPC-H `lineitem` table generated
  through DuckDB’s `tpch` extension when needed.
- The raw socket benchmark uses payload sizes in bytes, not rows,
  because it is intentionally outside the row/RPC contract.
- `quack` is installed from `core_nightly` during the run if it is not
  already available.
