suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(parallel)
}))

ducknng_bench_sql_quote <- function(x) {
  paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
}

ducknng_bench_open_duckdb <- function(dbdir = ":memory:", allow_unsigned_extensions = FALSE) {
  config <- if (allow_unsigned_extensions) {
    list(allow_unsigned_extensions = "true")
  } else {
    list()
  }
  DBI::dbConnect(duckdb::duckdb(config = config), dbdir = dbdir)
}

ducknng_bench_open_ducknng_db <- function(dbdir = ":memory:") {
  ducknng_bench_open_duckdb(dbdir, allow_unsigned_extensions = TRUE)
}

ducknng_bench_safe_disconnect <- function(con) {
  if (!is.null(con) && DBI::dbIsValid(con)) {
    suppressWarnings(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE))
  }
  invisible(NULL)
}

ducknng_bench_load_ducknng <- function(con, ext_path) {
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  invisible(con)
}

ducknng_bench_load_quack <- function(con) {
  try(DBI::dbExecute(con, "INSTALL quack FROM core_nightly"), silent = TRUE)
  DBI::dbExecute(con, "LOAD quack")
  invisible(con)
}

ducknng_bench_set_single_thread <- function(con) {
  DBI::dbExecute(con, "PRAGMA threads=1")
  DBI::dbExecute(con, "SET enable_progress_bar = false")
  invisible(con)
}

ducknng_bench_ensure_quack_available <- function() {
  con <- ducknng_bench_open_duckdb(":memory:")
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  ducknng_bench_load_quack(con)
  invisible(TRUE)
}

ducknng_bench_ensure_tpch_db <- function(path, sf, target_rows) {
  con <- ducknng_bench_open_duckdb(path, allow_unsigned_extensions = TRUE)
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  count <- NA_real_
  if (DBI::dbExistsTable(con, "lineitem")) {
    count <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM lineitem")$n[[1]])
  }
  if (!is.finite(count) || count < target_rows) {
    try(DBI::dbExecute(con, "INSTALL tpch"), silent = TRUE)
    DBI::dbExecute(con, "LOAD tpch")
    for (tbl in c("lineitem", "orders", "customer", "partsupp", "supplier", "part", "nation", "region")) {
      if (DBI::dbExistsTable(con, tbl)) {
        DBI::dbExecute(con, sprintf("DROP TABLE %s", tbl))
      }
    }
    DBI::dbExecute(con, sprintf("CALL dbgen(sf=%d)", sf))
    count <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM lineitem")$n[[1]])
  }
  stopifnot(is.finite(count), count >= target_rows)
  invisible(count)
}

ducknng_bench_aggregate_validation_sql <- function(source_sql) {
  paste(
    "SELECT",
    "count(*) AS row_count,",
    "sum(l_orderkey) AS sum_orderkey,",
    "sum(l_partkey) AS sum_partkey,",
    "sum(l_suppkey) AS sum_suppkey,",
    "sum(l_linenumber) AS sum_linenumber,",
    "sum(l_quantity) AS sum_quantity,",
    "sum(l_extendedprice) AS sum_extendedprice,",
    "sum(l_discount) AS sum_discount,",
    "sum(l_tax) AS sum_tax,",
    "min(l_returnflag) AS min_returnflag,",
    "max(l_linestatus) AS max_linestatus,",
    "min(l_shipdate) AS min_shipdate,",
    "max(l_commitdate) AS max_commitdate,",
    "max(l_receiptdate) AS max_receiptdate,",
    "max(length(l_shipinstruct)) AS shipinstruct_len,",
    "max(length(l_shipmode)) AS shipmode_len,",
    "sum(length(l_comment)) AS comment_len",
    "FROM", source_sql
  )
}

ducknng_bench_normalize_result <- function(df) {
  stopifnot(nrow(df) == 1L)
  out <- vector("list", ncol(df))
  names(out) <- names(df)
  for (i in seq_along(df)) {
    value <- df[[i]][[1]]
    if (inherits(value, "Date")) {
      out[[i]] <- as.character(value)
    } else if (is.numeric(value)) {
      out[[i]] <- sprintf("%.15g", as.numeric(value))
    } else {
      out[[i]] <- as.character(value)
    }
  }
  out
}

ducknng_bench_check_result <- function(actual, expected, expected_rows) {
  actual_row_count <- as.numeric(actual$row_count[[1]])
  stopifnot(identical(actual_row_count, as.numeric(expected_rows)))
  stopifnot(identical(
    ducknng_bench_normalize_result(actual),
    ducknng_bench_normalize_result(expected)
  ))
  invisible(TRUE)
}

ducknng_bench_local_baseline <- function(con, rows) {
  sql <- ducknng_bench_aggregate_validation_sql(
    sprintf("(SELECT * FROM lineitem LIMIT %d) AS lineitem_subset", rows)
  )
  DBI::dbGetQuery(con, sql)
}

ducknng_bench_time_query <- function(con, sql, expected, expected_rows) {
  result <- NULL
  elapsed <- system.time({
    result <- DBI::dbGetQuery(con, sql)
  })[["elapsed"]]
  ducknng_bench_check_result(result, expected, expected_rows)
  elapsed
}

ducknng_bench_ducknng_query_sql <- function(url, rows) {
  remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
  source_sql <- sprintf(
    "ducknng_query_rpc(%s, %s, 0::UBIGINT)",
    ducknng_bench_sql_quote(url), ducknng_bench_sql_quote(remote_sql)
  )
  ducknng_bench_aggregate_validation_sql(source_sql)
}

ducknng_bench_quack_query_sql <- function(uri, token, rows) {
  remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
  source_sql <- sprintf(
    "quack_query(%s, %s, token=%s)",
    ducknng_bench_sql_quote(uri),
    ducknng_bench_sql_quote(remote_sql),
    ducknng_bench_sql_quote(token)
  )
  ducknng_bench_aggregate_validation_sql(source_sql)
}

ducknng_bench_collect_rpc_timings <- function(system_name, protocol_name, transport_name,
    dataset_name, client_con, sql_builder, rows, repetitions, baselines) {
  results <- vector("list", length(rows))
  for (i in seq_along(rows)) {
    row_count <- rows[[i]]
    sql <- sql_builder(row_count)
    invisible(ducknng_bench_time_query(client_con, sql, baselines[[as.character(row_count)]], row_count))
    timings <- vapply(seq_len(repetitions), function(rep_idx) {
      ducknng_bench_time_query(client_con, sql, baselines[[as.character(row_count)]], row_count)
    }, numeric(1))
    results[[i]] <- data.frame(
      benchmark = "bulk_transfer_lineitem_limit",
      dataset = dataset_name,
      system = system_name,
      protocol = protocol_name,
      transport = transport_name,
      rows = row_count,
      repetitions = repetitions,
      median_seconds = round(stats::median(timings), 3),
      min_seconds = round(min(timings), 3),
      max_seconds = round(max(timings), 3),
      timings_seconds = paste(sprintf("%.3f", timings), collapse = ","),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, results)
}

ducknng_bench_build_rpc_url_template <- function(transport) {
  switch(transport,
    http = "http://127.0.0.1:0/_ducknng",
    tcp = "tcp://127.0.0.1:0",
    ws = "ws://127.0.0.1:0/_ducknng",
    ipc = paste0("ipc://", tempfile(pattern = "ducknng_bulk_rpc_", tmpdir = "/tmp", fileext = ".ipc")),
    stop("unsupported transport: ", transport)
  )
}

ducknng_bench_run_ducknng_rpc_transport <- function(transport, rows, repetitions, baselines,
    db_path, ext_path, dataset_name) {
  service_name <- paste0("bench_bulk_", transport)
  listen_template <- ducknng_bench_build_rpc_url_template(transport)
  server <- ducknng_bench_open_ducknng_db(db_path)
  client <- NULL
  on.exit(ducknng_bench_safe_disconnect(client), add = TRUE)
  on.exit({
    if (!is.null(server) && DBI::dbIsValid(server)) {
      suppressWarnings(try(
        DBI::dbGetQuery(server, sprintf(
          "SELECT ducknng_stop_server(%s) AS ok",
          ducknng_bench_sql_quote(service_name)
        )), silent = TRUE
      ))
    }
    ducknng_bench_safe_disconnect(server)
  }, add = TRUE)
  ducknng_bench_set_single_thread(server)
  ducknng_bench_load_ducknng(server, ext_path)
  stopifnot(isTRUE(DBI::dbGetQuery(
    server,
    sprintf(
      "SELECT ducknng_start_server(%s, %s, 1, 134217728, 300000, 0::UBIGINT) AS ok",
      ducknng_bench_sql_quote(service_name),
      ducknng_bench_sql_quote(listen_template)
    )
  )$ok[[1]]))
  actual_url <- DBI::dbGetQuery(
    server,
    sprintf("SELECT listen FROM ducknng_list_servers() WHERE name = %s",
      ducknng_bench_sql_quote(service_name))
  )$listen[[1]]
  Sys.sleep(1)
  client <- ducknng_bench_open_ducknng_db(":memory:")
  ducknng_bench_set_single_thread(client)
  ducknng_bench_load_ducknng(client, ext_path)
  ducknng_bench_collect_rpc_timings(
    system_name = "ducknng",
    protocol_name = "rpc",
    transport_name = transport,
    dataset_name = dataset_name,
    client_con = client,
    sql_builder = function(n) ducknng_bench_ducknng_query_sql(actual_url, n),
    rows = rows,
    repetitions = repetitions,
    baselines = baselines
  )
}

ducknng_bench_run_quack_http <- function(rows, repetitions, baselines,
    db_path, dataset_name, quack_uri, quack_token) {
  server <- ducknng_bench_open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  client <- NULL
  on.exit(ducknng_bench_safe_disconnect(client), add = TRUE)
  on.exit(ducknng_bench_safe_disconnect(server), add = TRUE)
  ducknng_bench_set_single_thread(server)
  ducknng_bench_load_quack(server)
  DBI::dbExecute(
    server,
    sprintf("CALL quack_serve(%s, token=%s)",
      ducknng_bench_sql_quote(quack_uri),
      ducknng_bench_sql_quote(quack_token))
  )
  Sys.sleep(1)
  client <- ducknng_bench_open_duckdb(":memory:")
  ducknng_bench_set_single_thread(client)
  ducknng_bench_load_quack(client)
  ducknng_bench_collect_rpc_timings(
    system_name = "quack",
    protocol_name = "quack_query",
    transport_name = "http",
    dataset_name = dataset_name,
    client_con = client,
    sql_builder = function(n) ducknng_bench_quack_query_sql(quack_uri, quack_token, n),
    rows = rows,
    repetitions = repetitions,
    baselines = baselines
  )
}

ducknng_bench_start_raw_echo_server <- function(url_template, expected_requests, timeout_ms, ext_path) {
  ready_file <- tempfile(pattern = "ducknng_raw_echo_", tmpdir = "/tmp", fileext = ".url")
  done_file <- tempfile(pattern = "ducknng_raw_echo_", tmpdir = "/tmp", fileext = ".done")
  log_file <- tempfile(pattern = "ducknng_raw_echo_", tmpdir = "/tmp", fileext = ".log")
  script_file <- tempfile(pattern = "ducknng_raw_echo_", tmpdir = "/tmp", fileext = ".R")
  unlink(c(ready_file, done_file, log_file, script_file))
  writeLines(c(
    "suppressPackageStartupMessages({library(DBI);library(duckdb)})",
    "source('bench/rpc_bulk_compare_support.R', local = TRUE)",
    sprintf("ready_file <- %s", deparse(ready_file)),
    sprintf("done_file <- %s", deparse(done_file)),
    sprintf("ext_path <- %s", deparse(ext_path)),
    sprintf("url_template <- %s", deparse(url_template)),
    sprintf("expected_requests <- %dL", as.integer(expected_requests)),
    sprintf("timeout_ms <- %dL", as.integer(timeout_ms)),
    "con <- ducknng_bench_open_ducknng_db(':memory:')",
    "on.exit({ ducknng_bench_safe_disconnect(con); writeLines('done', done_file) }, add = TRUE)",
    "ducknng_bench_load_ducknng(con, ext_path)",
    "pair_id <- DBI::dbGetQuery(con, \"SELECT (ducknng_open_socket('pair')).socket_id AS socket_id\")$socket_id[[1]]",
    paste0(
      "listen_sql <- sprintf(\"SELECT sock.ok AS ok, sock.url AS url ",
      "FROM (SELECT ducknng_listen_socket(%s::UBIGINT, %s, 134217728, 0::UBIGINT) AS sock) q\", ",
      "pair_id, ducknng_bench_sql_quote(url_template))"
    ),
    "listen_result <- DBI::dbGetQuery(con, listen_sql)",
    "stopifnot(isTRUE(listen_result$ok[[1]]), nzchar(listen_result$url[[1]]))",
    "writeLines(listen_result$url[[1]], ready_file)",
    "for (i in seq_len(expected_requests)) {",
    "  payload_hex <- NULL",
    "  for (attempt in seq_len(600L)) {",
    "    recv_sql <- sprintf(\"SELECT sock.ok AS ok, hex(sock.payload) AS payload_hex FROM (SELECT ducknng_recv_socket_raw(%s::UBIGINT, 100) AS sock) q\", pair_id)",
    "    recv_res <- DBI::dbGetQuery(con, recv_sql)",
    "    if (nrow(recv_res) == 1L && isTRUE(as.logical(recv_res$ok[[1]])) && !is.na(recv_res$payload_hex[[1]])) {",
    "      payload_hex <- recv_res$payload_hex[[1]]",
    "      break",
    "    }",
    "    Sys.sleep(0.05)",
    "  }",
    "  stopifnot(!is.null(payload_hex), nzchar(payload_hex))",
    "  ok <- DBI::dbGetQuery(con, sprintf(\"SELECT (ducknng_send_socket_raw(%s::UBIGINT, from_hex('%s'), %d)).ok AS ok\", pair_id, payload_hex, timeout_ms))$ok[[1]]",
    "  stopifnot(isTRUE(as.logical(ok)))",
    "}",
    "invisible(DBI::dbGetQuery(con, sprintf(\"SELECT (ducknng_close_socket(%s::UBIGINT)).ok AS ok\", pair_id)))"
  ), script_file)
  cmd <- sprintf(
    "cd %s && Rscript %s > %s 2>&1 & echo $!",
    shQuote(normalizePath(getwd())),
    shQuote(script_file),
    shQuote(log_file)
  )
  pid <- suppressWarnings(as.integer(system2("bash", c("-lc", shQuote(cmd)), stdout = TRUE)))
  actual_url <- NULL
  for (i in seq_len(300L)) {
    if (file.exists(ready_file)) {
      actual_url <- readLines(ready_file, warn = FALSE)
      if (length(actual_url) >= 1L && nzchar(actual_url[[1]])) break
    }
    if (file.exists(done_file)) break
    Sys.sleep(0.1)
  }
  if (is.null(actual_url) || length(actual_url) == 0L || !nzchar(actual_url[[1]])) {
    log_text <- if (file.exists(log_file)) paste(readLines(log_file, warn = FALSE), collapse = "\n") else ""
    if (is.finite(pid)) suppressWarnings(try(tools::pskill(pid), silent = TRUE))
    stop(paste(c("ducknng raw echo server did not publish a listen URL", log_text), collapse = "\n"))
  }
  list(pid = pid, url = actual_url[[1]], ready_file = ready_file, done_file = done_file,
       log_file = log_file, script_file = script_file)
}

ducknng_bench_wait_raw_echo_server <- function(server, timeout_seconds = 60) {
  deadline <- Sys.time() + timeout_seconds
  while (Sys.time() < deadline) {
    if (!is.null(server$done_file) && file.exists(server$done_file)) return(invisible(TRUE))
    Sys.sleep(0.1)
  }
  if (is.finite(server$pid)) suppressWarnings(try(tools::pskill(server$pid), silent = TRUE))
  log_text <- if (!is.null(server$log_file) && file.exists(server$log_file)) {
    paste(readLines(server$log_file, warn = FALSE), collapse = "\n")
  } else {
    ""
  }
  stop(paste(c("ducknng raw echo server did not finish", log_text), collapse = "\n"))
}

ducknng_bench_cleanup_raw_echo_server <- function(server) {
  if (!is.null(server)) {
    suppressWarnings(try(ducknng_bench_wait_raw_echo_server(server), silent = TRUE))
    for (path in c(server$ready_file, server$done_file, server$log_file, server$script_file)) {
      if (!is.null(path) && file.exists(path)) unlink(path)
    }
  }
  invisible(NULL)
}

ducknng_bench_raw_echo_url_template <- function(transport) {
  switch(transport,
    ipc = paste0("ipc://", tempfile(pattern = "ducknng_raw_echo_", tmpdir = "/tmp", fileext = ".ipc")),
    tcp = "tcp://127.0.0.1:0",
    ws = "ws://127.0.0.1:0/raw_echo",
    stop("unsupported raw transport: ", transport)
  )
}

ducknng_bench_run_raw_reqrep_echo_transport <- function(transport, payload_bytes, repetitions, ext_path,
    timeout_ms = 30000L) {
  stopifnot(length(payload_bytes) > 0L, all(payload_bytes > 0L), repetitions > 0L)
  con <- ducknng_bench_open_ducknng_db(":memory:")
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  ducknng_bench_load_ducknng(con, ext_path)
  server_id <- DBI::dbGetQuery(con, "SELECT (ducknng_open_socket('pair')).socket_id AS socket_id")$socket_id[[1]]
  client_id <- DBI::dbGetQuery(con, "SELECT (ducknng_open_socket('pair')).socket_id AS socket_id")$socket_id[[1]]
  on.exit(suppressWarnings(try(DBI::dbGetQuery(con, sprintf(
    "SELECT (ducknng_close_socket(%s::UBIGINT)).ok AS ok", client_id
  )), silent = TRUE)), add = TRUE)
  on.exit(suppressWarnings(try(DBI::dbGetQuery(con, sprintf(
    "SELECT (ducknng_close_socket(%s::UBIGINT)).ok AS ok", server_id
  )), silent = TRUE)), add = TRUE)
  listen_result <- DBI::dbGetQuery(con, sprintf(
    "SELECT sock.ok AS ok, sock.url AS url FROM (SELECT ducknng_listen_socket(%s::UBIGINT, %s, 134217728, 0::UBIGINT) AS sock) q",
    server_id,
    ducknng_bench_sql_quote(ducknng_bench_raw_echo_url_template(transport))
  ))
  stopifnot(isTRUE(as.logical(listen_result$ok[[1]])), nzchar(listen_result$url[[1]]))
  dial_ok <- FALSE
  for (attempt in seq_len(50L)) {
    dial_ok <- isTRUE(as.logical(DBI::dbGetQuery(con, sprintf(
      "SELECT (ducknng_dial_socket(%s::UBIGINT, %s, %d, 0::UBIGINT)).ok AS ok",
      client_id,
      ducknng_bench_sql_quote(listen_result$url[[1]]),
      timeout_ms
    ))$ok[[1]]))
    if (dial_ok) break
    Sys.sleep(0.1)
  }
  stopifnot(dial_ok)
  roundtrip_once <- function(bytes) {
    recv_aio <- DBI::dbGetQuery(con, sprintf(
      "SELECT ducknng_recv_socket_raw_aio(%s::UBIGINT, %d) AS aio",
      server_id, timeout_ms
    ))$aio[[1]]
    send_ok <- DBI::dbGetQuery(con, sprintf(
      "SELECT (ducknng_send_socket_raw(%s::UBIGINT, repeat('x', %d)::BLOB, %d)).ok AS ok",
      client_id, bytes, timeout_ms
    ))$ok[[1]]
    stopifnot(isTRUE(as.logical(send_ok)))
    payload_hex <- DBI::dbGetQuery(con, sprintf(
      "SELECT hex(frame) AS payload_hex FROM ducknng_aio_collect(list_value(%s::UBIGINT), %d)",
      recv_aio, timeout_ms
    ))$payload_hex[[1]]
    stopifnot(!is.na(payload_hex), nchar(payload_hex) == 2L * bytes)
    echo_ok <- DBI::dbGetQuery(con, sprintf(
      "SELECT (ducknng_send_socket_raw(%s::UBIGINT, from_hex('%s'), %d)).ok AS ok",
      server_id, payload_hex, timeout_ms
    ))$ok[[1]]
    stopifnot(isTRUE(as.logical(echo_ok)))
    DBI::dbGetQuery(con, sprintf(
      "SELECT octet_length((ducknng_recv_socket_raw(%s::UBIGINT, %d)).payload) AS payload_len",
      client_id, timeout_ms
    ))$payload_len[[1]]
  }
  results <- vector("list", length(payload_bytes))
  for (i in seq_along(payload_bytes)) {
    bytes <- as.integer(payload_bytes[[i]])
    warmup <- roundtrip_once(bytes)
    stopifnot(identical(as.numeric(warmup), as.numeric(bytes)))
    timings <- vapply(seq_len(repetitions), function(rep_idx) {
      elapsed <- system.time({
        got <- roundtrip_once(bytes)
      })[["elapsed"]]
      stopifnot(identical(as.numeric(got), as.numeric(bytes)))
      elapsed
    }, numeric(1))
    mib_per_sec <- (2 * bytes / (1024^2)) / timings
    results[[i]] <- data.frame(
      benchmark = "raw_socket_echo",
      system = "ducknng",
      protocol = "raw_pair_echo",
      transport = transport,
      payload_bytes = bytes,
      repetitions = repetitions,
      median_seconds = round(stats::median(timings), 3),
      min_seconds = round(min(timings), 3),
      max_seconds = round(max(timings), 3),
      median_roundtrip_mib_per_sec = round(stats::median(mib_per_sec), 3),
      timings_seconds = paste(sprintf("%.3f", timings), collapse = ","),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, results)
}

ducknng_bench_capture_cmd <- function(command) {
  out <- suppressWarnings(try(system2("bash", c("-lc", shQuote(command)), stdout = TRUE, stderr = FALSE), silent = TRUE))
  if (inherits(out, "try-error") || length(out) == 0L) return(NA_character_)
  trimws(paste(out, collapse = "\n"))
}

ducknng_bench_machine_details <- function(ext_path) {
  sys <- Sys.info()
  mem_total <- ducknng_bench_capture_cmd("awk '/MemTotal:/ {printf \"%.1f GiB\", $2/1024/1024}' /proc/meminfo")
  cpu_model <- ducknng_bench_capture_cmd("awk -F: '/model name/ {gsub(/^ +/, \"\", $2); print $2; exit}' /proc/cpuinfo")
  if (is.na(cpu_model) || !nzchar(cpu_model)) {
    cpu_model <- ducknng_bench_capture_cmd("sysctl -n machdep.cpu.brand_string 2>/dev/null")
  }
  data.frame(
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    hostname = unname(sys[["nodename"]]),
    sysname = unname(sys[["sysname"]]),
    release = unname(sys[["release"]]),
    machine = unname(sys[["machine"]]),
    cpu_model = cpu_model,
    logical_cores = parallel::detectCores(logical = TRUE),
    physical_cores = parallel::detectCores(logical = FALSE),
    memory_total = mem_total,
    r_version = R.version.string,
    duckdb_version = as.character(utils::packageVersion("duckdb")),
    ducknng_extension = normalizePath(ext_path, mustWork = FALSE),
    ducknng_git_commit = ducknng_bench_capture_cmd("git rev-parse --short HEAD"),
    quack_install_source = "INSTALL quack FROM core_nightly",
    stringsAsFactors = FALSE
  )
}

ducknng_bench_find_ext_path <- function() {
  candidates <- c(
    "build/release/ducknng.duckdb_extension",
    "../build/release/ducknng.duckdb_extension"
  )
  for (path in candidates) {
    if (file.exists(path)) return(normalizePath(path, mustWork = TRUE))
  }
  stop("ducknng benchmark could not find build/release/ducknng.duckdb_extension")
}

ducknng_bench_parse_int_csv <- function(text, default) {
  if (is.null(text) || !nzchar(text)) return(as.integer(default))
  as.integer(strsplit(text, ",", fixed = TRUE)[[1]])
}

ducknng_bench_run_bulk_compare <- function(
    repetitions = 5L,
    rows = c(100000L, 1000000L, 10000000L),
    raw_payload_bytes = c(1048576L, 4194304L, 16777216L),
    db_path = file.path(tempdir(), "ducknng_quack_tpch.duckdb"),
    quack_uri = Sys.getenv("DUCKNNG_QUACK_URI", unset = "quack:localhost:19494"),
    quack_token = Sys.getenv("DUCKNNG_QUACK_TOKEN", unset = "asdf"),
    ducknng_transports = c("http", "tcp", "ipc", "ws"),
    raw_transports = c("ipc", "tcp", "ws")) {
  stopifnot(repetitions > 0L, length(rows) > 0L, all(rows > 0L))
  stopifnot(length(raw_payload_bytes) > 0L, all(raw_payload_bytes > 0L))
  ext_path <- ducknng_bench_find_ext_path()
  max_rows <- max(rows)
  required_sf <- max(1L, as.integer(ceiling(max_rows / 6000000)))
  dataset_name <- sprintf("tpch_sf%d.lineitem", required_sf)

  ducknng_bench_ensure_quack_available()
  total_lineitem_rows <- ducknng_bench_ensure_tpch_db(db_path, required_sf, max_rows)

  baseline_con <- ducknng_bench_open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  on.exit(ducknng_bench_safe_disconnect(baseline_con), add = TRUE)
  ducknng_bench_set_single_thread(baseline_con)
  baselines <- setNames(lapply(rows, function(n) ducknng_bench_local_baseline(baseline_con, n)), as.character(rows))

  ducknng_rpc_results <- do.call(rbind, lapply(ducknng_transports, function(transport) {
    ducknng_bench_run_ducknng_rpc_transport(
      transport = transport,
      rows = rows,
      repetitions = repetitions,
      baselines = baselines,
      db_path = db_path,
      ext_path = ext_path,
      dataset_name = dataset_name
    )
  }))

  quack_results <- ducknng_bench_run_quack_http(
    rows = rows,
    repetitions = repetitions,
    baselines = baselines,
    db_path = db_path,
    dataset_name = dataset_name,
    quack_uri = quack_uri,
    quack_token = quack_token
  )

  raw_results <- do.call(rbind, lapply(raw_transports, function(transport) {
    ducknng_bench_run_raw_reqrep_echo_transport(
      transport = transport,
      payload_bytes = raw_payload_bytes,
      repetitions = repetitions,
      ext_path = ext_path
    )
  }))

  http_rows <- ducknng_rpc_results[ducknng_rpc_results$transport == "http", c("rows", "median_seconds")]
  names(http_rows)[2] <- "ducknng_http_median_seconds"
  quack_rows <- quack_results[, c("rows", "median_seconds")]
  names(quack_rows)[2] <- "quack_http_median_seconds"
  http_vs_quack <- merge(http_rows, quack_rows, by = "rows", all = TRUE)
  http_vs_quack$ducknng_over_quack_ratio <- round(
    http_vs_quack$ducknng_http_median_seconds / http_vs_quack$quack_http_median_seconds,
    3
  )

  metadata <- cbind(
    ducknng_bench_machine_details(ext_path),
    data.frame(
      dataset = dataset_name,
      lineitem_rows_available = total_lineitem_rows,
      repetitions = repetitions,
      raw_payload_bytes = paste(raw_payload_bytes, collapse = ","),
      ducknng_transports = paste(ducknng_transports, collapse = ","),
      raw_transports = paste(raw_transports, collapse = ","),
      quack_uri = quack_uri,
      stringsAsFactors = FALSE
    )
  )

  list(
    metadata = metadata,
    rpc_results = rbind(ducknng_rpc_results, quack_results),
    raw_results = raw_results,
    http_vs_quack = http_vs_quack
  )
}
