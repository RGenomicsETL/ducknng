suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
}))

sql_quote <- function(x) {
  paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
}

parse_mode <- function(args) {
  if (length(args) == 0L) {
    return(list(mode = "micro", args = args))
  }
  if (args[[1]] %in% c("micro", "bulk_compare", "bulk-compare", "compare")) {
    mode <- switch(args[[1]],
      "bulk-compare" = "bulk_compare",
      "compare" = "bulk_compare",
      args[[1]]
    )
    return(list(mode = mode, args = args[-1]))
  }
  list(mode = "micro", args = args)
}

open_duckdb <- function(dbdir = ":memory:", allow_unsigned_extensions = FALSE) {
  config <- if (allow_unsigned_extensions) {
    list(allow_unsigned_extensions = "true")
  } else {
    list()
  }
  DBI::dbConnect(duckdb::duckdb(config = config), dbdir = dbdir)
}

run_micro_bench <- function(args) {
  suppressWarnings(suppressPackageStartupMessages({
    library(nanonext)
    library(nanoarrow)
  }))

  iterations <- if (length(args) >= 1L) as.integer(args[[1]]) else 100L
  clients <- if (length(args) >= 2L) as.integer(args[[2]]) else 4L
  stopifnot(iterations > 0L, clients > 0L)

  u32le <- function(x) writeBin(as.integer(x), raw(), size = 4L, endian = "little")
  u64le <- function(x) {
    x <- as.double(x)
    c(u32le(x %% 2^32), u32le(floor(x / 2^32)))
  }
  read_u32le <- function(buf, offset) sum(as.double(as.integer(buf[offset + 0:3])) * 256^(0:3))
  read_u64le <- function(buf, offset) read_u32le(buf, offset) + 2^32 * read_u32le(buf, offset + 4)
  encode_call <- function(name, payload = raw(), flags = 0L) {
    name_raw <- charToRaw(name)
    c(as.raw(1L), as.raw(1L), u32le(flags), u32le(length(name_raw)), u32le(0),
      u64le(length(payload)), name_raw, payload)
  }
  encode_manifest_request <- function() {
    c(as.raw(1L), as.raw(0L), u32le(0), u32le(0), u32le(0), u64le(0))
  }
  encode_query_open_request <- function(sql, correlation_id = NULL, serialization_mode = NULL) {
    con <- rawConnection(raw(), open = "r+")
    on.exit(close(con))
    write_nanoarrow(
      data.frame(
        sql = sql,
        batch_rows = NA_character_,
        batch_bytes = NA_character_,
        correlation_id = if (is.null(correlation_id)) NA_character_ else correlation_id,
        serialization_mode = if (is.null(serialization_mode)) NA_character_ else serialization_mode,
        stringsAsFactors = FALSE
      ),
      con
    )
    encode_call("query_open", rawConnectionValue(con))
  }
  encode_session_control <- function(method, session_id, session_token, correlation_id = NULL) {
    suffix <- if (is.null(correlation_id)) "" else paste0(',"correlation_id":"', correlation_id, '"')
    json <- paste0(
      '{"session_id":', format(session_id, scientific = FALSE, trim = TRUE),
      ',"session_token":"', session_token, '"', suffix, "}"
    )
    encode_call(method, charToRaw(json))
  }
  decode_frame <- function(buf) {
    name_len <- read_u32le(buf, 7)
    error_len <- read_u32le(buf, 11)
    payload_len <- read_u64le(buf, 15)
    name_start <- 23L
    error_start <- name_start + name_len
    payload_start <- error_start + error_len
    payload_end <- payload_start + payload_len - 1L
    list(
      version = as.integer(buf[1]),
      type = as.integer(buf[2]),
      flags = read_u32le(buf, 3),
      name = if (name_len > 0) rawToChar(buf[name_start:(error_start - 1L)]) else "",
      error = if (error_len > 0) rawToChar(buf[error_start:(payload_start - 1L)]) else "",
      payload = if (payload_len > 0) buf[payload_start:payload_end] else raw()
    )
  }
  json_get_string <- function(json, key) {
    m <- regexec(sprintf('"%s":"([^"]*)"', key), json)
    parts <- regmatches(json, m)[[1]]
    if (length(parts) < 2L) NA_character_ else parts[2]
  }
  json_get_number <- function(json, key) {
    m <- regexec(sprintf('"%s":([0-9]+)', key), json)
    parts <- regmatches(json, m)[[1]]
    if (length(parts) < 2L) NA_real_ else as.numeric(parts[2])
  }
  rpc_roundtrip <- function(sock, frame) {
    stopifnot(nanonext::send(sock, frame, mode = "raw", block = 5000L) == 0)
    decode_frame(nanonext::recv(sock, mode = "raw", block = 5000L))
  }
  bench_case <- function(name, action, iterations) {
    elapsed <- system.time({
      for (i in seq_len(iterations)) action()
    })[["elapsed"]]
    data.frame(
      benchmark = name,
      iterations = iterations,
      clients = 1L,
      total_ms = as.integer(round(elapsed * 1000)),
      per_iter_ms = round((elapsed * 1000) / iterations, 3),
      stringsAsFactors = FALSE
    )
  }
  bench_parallel_sessions <- function(url, iterations, clients) {
    elapsed <- system.time({
      cl <- parallel::makeCluster(clients, type = "PSOCK")
      on.exit(parallel::stopCluster(cl), add = TRUE)
      parallel::clusterEvalQ(cl, suppressWarnings(suppressPackageStartupMessages({
        library(nanonext)
        library(nanoarrow)
      })))
      parallel::clusterExport(cl, c(
        "url", "iterations",
        "u32le", "u64le", "read_u32le", "read_u64le",
        "encode_call", "encode_query_open_request", "encode_session_control",
        "decode_frame", "json_get_string", "json_get_number", "rpc_roundtrip"
      ), envir = environment())
      ok <- parallel::parLapply(cl, seq_len(clients), function(worker_id) {
        sock <- nanonext::socket("req", dial = url, autostart = NA)
        on.exit(close(sock), add = TRUE)
        for (i in seq_len(iterations)) {
          open <- rpc_roundtrip(sock, encode_query_open_request(
            sprintf("SELECT %d AS worker_id, %d AS iter", worker_id, i),
            correlation_id = sprintf("open-%d-%d", worker_id, i)
          ))
          payload_text <- rawToChar(open$payload)
          session_id <- json_get_number(payload_text, "session_id")
          session_token <- json_get_string(payload_text, "session_token")
          fetch <- rpc_roundtrip(sock, encode_session_control(
            "fetch", session_id, session_token,
            sprintf("fetch-%d-%d", worker_id, i)
          ))
          if (fetch$type != 2L) stop("fetch failed")
          invisible(rpc_roundtrip(sock, encode_session_control(
            "close", session_id, session_token,
            sprintf("close-%d-%d", worker_id, i)
          )))
        }
        TRUE
      })
      stopifnot(all(vapply(ok, isTRUE, logical(1))))
    })[["elapsed"]]
    data.frame(
      benchmark = "parallel_sessions_arrow",
      iterations = iterations,
      clients = clients,
      total_ms = as.integer(round(elapsed * 1000)),
      per_iter_ms = round((elapsed * 1000) / (iterations * clients), 3),
      stringsAsFactors = FALSE
    )
  }

  ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
  ipc_path <- tempfile(pattern = "ducknng_bench_", tmpdir = "/tmp", fileext = ".ipc")
  ipc_url <- paste0("ipc://", ipc_path)

  drv <- duckdb::duckdb(config = list(allow_unsigned_extensions = "true"))
  con <- DBI::dbConnect(drv, dbdir = ":memory:")
  sock <- NULL
  on.exit({
    if (!is.null(sock)) {
      try(close(sock), silent = TRUE)
    }
    try(DBI::dbGetQuery(con, "SELECT ducknng_stop_server('bench')"), silent = TRUE)
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  }, add = TRUE)
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  DBI::dbGetQuery(con, sprintf(
    "SELECT ducknng_start_server('bench', '%s', 1, 134217728, 300000, 0)",
    ipc_url
  ))
  Sys.sleep(1)

  sock <- nanonext::socket("req", dial = ipc_url, autostart = NA)

  results <- list()
  results[[length(results) + 1L]] <- bench_case(
    "manifest_roundtrip",
    function() rpc_roundtrip(sock, encode_manifest_request()),
    iterations
  )
  results[[length(results) + 1L]] <- bench_case(
    "session_roundtrip_arrow",
    function() {
      open <- rpc_roundtrip(sock, encode_query_open_request("SELECT 1 AS x"))
      payload_text <- rawToChar(open$payload)
      session_id <- json_get_number(payload_text, "session_id")
      session_token <- json_get_string(payload_text, "session_token")
      rpc_roundtrip(sock, encode_session_control("fetch", session_id, session_token, "fetch-arrow"))
      rpc_roundtrip(sock, encode_session_control("close", session_id, session_token, "close-arrow"))
      invisible(NULL)
    },
    iterations
  )
  results[[length(results) + 1L]] <- bench_parallel_sessions(
    ipc_url,
    max(1L, iterations %/% max(1L, clients)),
    clients
  )

  print(do.call(rbind, results), row.names = FALSE)
}

run_bulk_compare <- function(args) {
  repetitions <- if (length(args) >= 1L) as.integer(args[[1]]) else 5L
  rows <- if (length(args) >= 2L) {
    as.integer(strsplit(args[[2]], ",", fixed = TRUE)[[1]])
  } else {
    c(100000L, 1000000L, 10000000L)
  }
  db_path <- if (length(args) >= 3L) args[[3]] else file.path(tempdir(), "ducknng_quack_tpch.duckdb")
  stopifnot(repetitions > 0L, length(rows) > 0L, all(rows > 0L))

  ducknng_ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
  ducknng_url <- Sys.getenv("DUCKNNG_BENCH_HTTP_URL", unset = "http://127.0.0.1:18495/_ducknng")
  quack_uri <- Sys.getenv("DUCKNNG_QUACK_URI", unset = "quack:localhost:19494")
  quack_token <- Sys.getenv("DUCKNNG_QUACK_TOKEN", unset = "asdf")
  max_rows <- max(rows)
  required_sf <- max(1L, as.integer(ceiling(max_rows / 6000000)))

  ensure_quack_available <- function() {
    con <- open_duckdb(":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    try(DBI::dbExecute(con, "INSTALL quack FROM core_nightly"), silent = TRUE)
    DBI::dbExecute(con, "LOAD quack")
    invisible(TRUE)
  }

  ensure_tpch_db <- function(path, sf, target_rows) {
    con <- open_duckdb(path, allow_unsigned_extensions = TRUE)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
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

  set_single_thread <- function(con) {
    DBI::dbExecute(con, "PRAGMA threads=1")
    DBI::dbExecute(con, "SET enable_progress_bar = false")
    invisible(con)
  }

  safe_disconnect <- function(con) {
    if (!is.null(con) && DBI::dbIsValid(con)) {
      suppressWarnings(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE))
    }
    invisible(NULL)
  }

  safe_ducknng_stop <- function(con, service_name) {
    if (!is.null(con) && DBI::dbIsValid(con)) {
      suppressWarnings(try(
        DBI::dbGetQuery(con, sprintf("SELECT ducknng_stop_server(%s) AS ok", sql_quote(service_name))),
        silent = TRUE
      ))
    }
    invisible(NULL)
  }

  open_ducknng_db <- function(dbdir = ":memory:") {
    DBI::dbConnect(
      duckdb::duckdb(config = list(allow_unsigned_extensions = "true")),
      dbdir = dbdir
    )
  }

  load_ducknng <- function(con, ext_path) {
    DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
    invisible(con)
  }

  load_quack <- function(con) {
    try(DBI::dbExecute(con, "INSTALL quack FROM core_nightly"), silent = TRUE)
    DBI::dbExecute(con, "LOAD quack")
    invisible(con)
  }

  aggregate_validation_sql <- function(source_sql) {
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

  normalize_result <- function(df) {
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

  check_result <- function(actual, expected, expected_rows) {
    actual_row_count <- as.numeric(actual$row_count[[1]])
    stopifnot(identical(actual_row_count, as.numeric(expected_rows)))
    stopifnot(identical(normalize_result(actual), normalize_result(expected)))
    invisible(TRUE)
  }

  local_baseline <- function(con, rows) {
    sql <- aggregate_validation_sql(sprintf("(SELECT * FROM lineitem LIMIT %d) AS lineitem_subset", rows))
    DBI::dbGetQuery(con, sql)
  }

  ducknng_sql <- function(url, rows) {
    remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
    source_sql <- sprintf(
      "ducknng_query_rpc(%s, %s, 0::UBIGINT)",
      sql_quote(url), sql_quote(remote_sql)
    )
    aggregate_validation_sql(source_sql)
  }

  quack_sql <- function(uri, token, rows) {
    remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
    source_sql <- sprintf(
      "quack_query(%s, %s, token=%s)",
      sql_quote(uri), sql_quote(remote_sql), sql_quote(token)
    )
    aggregate_validation_sql(source_sql)
  }

  time_query <- function(con, sql, expected, expected_rows) {
    result <- NULL
    elapsed <- system.time({
      result <- DBI::dbGetQuery(con, sql)
    })[["elapsed"]]
    check_result(result, expected, expected_rows)
    elapsed
  }

  bench_protocol <- function(protocol, client_con, sql_builder, rows, repetitions, baselines) {
    results <- vector("list", length(rows))
    for (i in seq_along(rows)) {
      row_count <- rows[[i]]
      sql <- sql_builder(row_count)
      invisible(time_query(client_con, sql, baselines[[as.character(row_count)]], row_count))
      timings <- vapply(seq_len(repetitions), function(rep_idx) {
        time_query(client_con, sql, baselines[[as.character(row_count)]], row_count)
      }, numeric(1))
      results[[i]] <- data.frame(
        benchmark = "bulk_transfer_lineitem_limit",
        dataset = sprintf("tpch_sf%d.lineitem", required_sf),
        protocol = protocol,
        transport = if (protocol == "ducknng") "http" else "quack_http",
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

  ensure_quack_available()
  total_lineitem_rows <- ensure_tpch_db(db_path, required_sf, max_rows)

  baseline_con <- open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  on.exit(safe_disconnect(baseline_con), add = TRUE)
  set_single_thread(baseline_con)
  baselines <- setNames(lapply(rows, function(n) local_baseline(baseline_con, n)), as.character(rows))

  ducknng_server <- open_ducknng_db(db_path)
  on.exit(safe_ducknng_stop(ducknng_server, "bench_bulk_http"), add = TRUE)
  on.exit(safe_disconnect(ducknng_server), add = TRUE)
  set_single_thread(ducknng_server)
  load_ducknng(ducknng_server, ducknng_ext_path)
  stopifnot(isTRUE(DBI::dbGetQuery(
    ducknng_server,
    sprintf(
      "SELECT ducknng_start_server('bench_bulk_http', %s, 1, 134217728, 300000, 0::UBIGINT) AS ok",
      sql_quote(ducknng_url)
    )
  )$ok[[1]]))
  Sys.sleep(1)

  ducknng_client <- open_ducknng_db(":memory:")
  on.exit(safe_disconnect(ducknng_client), add = TRUE)
  set_single_thread(ducknng_client)
  load_ducknng(ducknng_client, ducknng_ext_path)
  ducknng_results <- bench_protocol(
    "ducknng",
    ducknng_client,
    function(n) ducknng_sql(ducknng_url, n),
    rows,
    repetitions,
    baselines
  )
  safe_ducknng_stop(ducknng_server, "bench_bulk_http")
  safe_disconnect(ducknng_client)
  safe_disconnect(ducknng_server)

  quack_server <- open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  on.exit(safe_disconnect(quack_server), add = TRUE)
  set_single_thread(quack_server)
  load_quack(quack_server)
  DBI::dbExecute(
    quack_server,
    sprintf("CALL quack_serve(%s, token=%s)", sql_quote(quack_uri), sql_quote(quack_token))
  )
  Sys.sleep(1)

  quack_client <- open_duckdb(":memory:")
  on.exit(safe_disconnect(quack_client), add = TRUE)
  set_single_thread(quack_client)
  load_quack(quack_client)
  quack_results <- bench_protocol(
    "quack",
    quack_client,
    function(n) quack_sql(quack_uri, quack_token, n),
    rows,
    repetitions,
    baselines
  )
  safe_disconnect(quack_client)
  safe_disconnect(quack_server)

  results <- rbind(ducknng_results, quack_results)
  attr(results, "lineitem_rows") <- total_lineitem_rows
  print(results, row.names = FALSE)
}

parsed <- parse_mode(commandArgs(trailingOnly = TRUE))
switch(parsed$mode,
  micro = run_micro_bench(parsed$args),
  bulk_compare = run_bulk_compare(parsed$args)
)
