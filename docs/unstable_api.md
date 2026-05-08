# DuckDB Unstable API Audit

This document records findings from a systematic review of all `unstable_*` API groups in
`duckdb_extension.h` and `duckdb.h`, with a recommendation per group on whether and how
`ducknng` should use each one.

---

## `unstable_new_arrow_functions`

**Functions:** `duckdb_to_arrow_schema`, `duckdb_data_chunk_to_arrow`,
`duckdb_schema_from_arrow`, `duckdb_data_chunk_from_arrow`,
`duckdb_destroy_arrow_converted_schema`

**What it does.** Converts between `duckdb_data_chunk` / `duckdb_logical_type` and
nanoarrow `ArrowSchema` / `ArrowArray` using DuckDB's own type mapping rather than a
hand-written translation table.

**Current situation.** `ducknng_ipc_out.c` builds Arrow IPC from DuckDB result chunks
using a manually maintained `ducknng_set_arrow_schema_type` switch. `ducknng_ipc_in.c`
decodes Arrow IPC and produces nanoarrow arrays that are later scanned back into DuckDB
vectors by `ducknng_sql_arrow.c`. Both translation paths are non-trivial and must track
every new DuckDB type that DuckDB adds.

**Recommendation: HIGH priority adoption.** `duckdb_to_arrow_schema` and
`duckdb_data_chunk_to_arrow` can replace the manual schema switch in `ducknng_ipc_out.c`,
eliminating the risk of missing new types. `duckdb_schema_from_arrow` and
`duckdb_data_chunk_from_arrow` can replace `ducknng_sql_arrow_schema_to_logical_type` and
the per-type vector write logic in `ducknng_sql_arrow.c`. Adopt in a dedicated refactor
pass; keep the existing path until the new one is tested.

---

## `unstable_new_string_functions`

**Functions:** `duckdb_valid_utf8_check(str, len)`, `duckdb_value_to_string`,
`duckdb_unsafe_vector_assign_string_element_len` (in the vector group)

**What it does.** `duckdb_valid_utf8_check` validates UTF-8 in-place.
`duckdb_unsafe_vector_assign_string_element_len` assigns a string element with a known
length, avoiding an internal `strlen` call.

**Current situation.** `ducknng_util.c` contains `ducknng_sql_bytes_look_text`, which
does its own UTF-8 validation. Several scan paths call
`duckdb_vector_assign_string_element` (which does `strlen` on every assign).

**Recommendation: MEDIUM priority.** Replace `ducknng_sql_bytes_look_text` with
`duckdb_valid_utf8_check`. Replace `duckdb_vector_assign_string_element` calls in hot
scan paths with `duckdb_unsafe_vector_assign_string_element_len` where the length is
already known. Both are one-line changes per call site.

---

## `unstable_new_table_function_functions`

**Functions:** `duckdb_table_function_get_client_context(bind_info, out_ctx)`

**What it does.** Retrieves the DuckDB client context from inside a table-function bind
callback, avoiding the need to pass a context pointer through `extra_info`.

**Current situation.** `ducknng_body_parse_bind`, `ducknng_ncurl_table_bind`, and other
TVF bind callbacks receive a `ducknng_sql_context *` via `duckdb_bind_get_extra_info`.
This works correctly; the existing pattern is not wrong.

**Recommendation: LOW priority.** The existing `extra_info` pattern is clear. Migrating
to `duckdb_table_function_get_client_context` would simplify registration (no need to
store the context pointer as extra info) but is not urgent. Revisit if the number of
table functions grows significantly.

---

## `unstable_new_scalar_function_functions`

**Functions:** `duckdb_scalar_function_set_bind`, `duckdb_scalar_function_bind_get_argument`,
`duckdb_scalar_function_get_client_context`, `duckdb_scalar_function_bind_set_bind_data`,
`duckdb_scalar_function_get_bind_data`

**What it does.** Adds a bind phase to scalar functions so they can inspect constant
arguments at planning time and constant-fold or specialise their execution path.

**Current situation.** All scalar functions in `ducknng` (`ducknng_frame_payload`,
`ducknng_frame_type`, etc.) perform all work at execute time.

**Recommendation: MEDIUM priority.** Functions that take a constant codec or format
name could validate and specialise at bind time rather than per-row. Not urgent for
current functions but worth using for any new scalar function that accepts a constant
string selector.

---

## `unstable_new_open_connect_functions`

**Functions:** `duckdb_connection_get_client_context`,
`duckdb_client_context_get_connection_id`, `duckdb_get_table_names`

**What it does.** Exposes the internal client context from a `duckdb_connection` handle,
and provides a connection-level unique ID.

**Current situation.** Not used. Session identification in `ducknng` uses its own
`ducknng_session` and `ducknng_service` structures keyed by connection handle.

**Recommendation: LOW priority.** `duckdb_client_context_get_connection_id` could
simplify session-key lookups. Not needed now.

---

## `unstable_new_vector_functions`

**Functions:** `duckdb_unsafe_vector_assign_string_element_len`, `duckdb_create_vector`,
`duckdb_vector_reference_vector`, `duckdb_create_selection_vector`

**What it does.** Provides lower-level vector manipulation: zero-copy vector references,
selection vectors for filtering without copying, and string assignment with explicit
length.

**Recommendation: MEDIUM priority for string length.** See string functions note above.
`duckdb_create_selection_vector` is not needed today; `ducknng` does not implement custom
pushdown operators.

---

## `unstable_new_file_system_api`

**Functions:** `duckdb_file_system_open`, `duckdb_file_handle_read`,
`duckdb_file_handle_write`, `duckdb_file_handle_seek`, `duckdb_file_handle_tell`,
`duckdb_file_handle_get_file_size`, `duckdb_file_handle_sync`, `duckdb_file_handle_close`

**What it does.** Provides a consumer interface for opening and operating on files through
DuckDB's virtual file system abstraction. `duckdb_file_system` is an opaque
`{ void *internal_ptr }` wrapping a C++ `FileSystem *`.

**Important limitation.** There is no `duckdb_register_file_system` or equivalent
registration callback in the C API. The C++ side has
`DatabaseInstance::AddFileSystem(unique_ptr<FileSystem>)` and a virtual `FileSystem` base
class, but neither is bridged to C. Implementing a custom `ducknng://body/...` path that
DuckDB dispatches to would require constructing a C++ vtable with the correct layout and
ABI — platform- and version-specific, and not viable from pure C.

**What the API is good for.** The consumer-side functions — `duckdb_file_system_open` and
`duckdb_file_handle_write` — can replace the raw `mkstemp`/`write`/`close` calls in the
Parquet temp-file path (`ducknng_body_parse_run_tempfile_reader`), routing through
DuckDB's FS abstraction for better portability.

**Recommendation: LOW priority.** Replace OS-level tempfile I/O with
`duckdb_file_system_open` + `duckdb_file_handle_write` in the Parquet branch. Keep for
its own follow-up change.

---

## `unstable_new_copy_functions_api`

**Functions:** Full COPY TO / COPY FROM extension API with bind, sink, and finalize
callbacks, plus `duckdb_copy_function_set_copy_from_function`.

**What it does.** Lets extensions implement custom `COPY ... TO/FROM 'file' (FORMAT
myformat)` dialects integrated into DuckDB's query planner.

**Recommendation: NOT applicable.** `ducknng` parses body bytes in a TVF bind callback,
not through `COPY`. No benefit from this group.

---

## `unstable_new_expression_functions`

**Functions:** `duckdb_expression_is_foldable`, `duckdb_expression_fold`

**What it does.** Lets the extension ask the planner whether a given expression is
constant and fold it to a value at bind time.

**Recommendation: LOW priority.** Useful alongside `unstable_new_scalar_function_functions`
if scalar functions acquire bind phases. Not needed independently.

---

## `unstable_new_catalog_interface`

**Functions:** `duckdb_client_context_get_catalog`, `duckdb_catalog_get_entry`,
`duckdb_catalog_get_type_name`

**What it does.** Exposes the DuckDB catalog for looking up types and objects by name
from within an extension.

**Recommendation: NOT applicable now.** `ducknng` does not inspect the user catalog.

---

## `unstable_new_logger_functions`

**Functions:** `duckdb_create_log_storage`, `duckdb_log_storage_set_write_log_entry`,
`duckdb_register_log_storage`

**What it does.** Lets extensions register a custom log sink that receives DuckDB's
internal log entries.

**Recommendation: LOW priority.** Could expose DuckDB-level log lines through the
`ducknng` observability surface. Not urgent.

---

## `unstable_new_config_options_functions`

**Functions:** `duckdb_create_config_option`, `duckdb_register_config_option`,
`duckdb_client_context_get_config_option`

**What it does.** Lets extensions declare named configuration options visible through
`SET ducknng.option = value` and readable in SQL.

**Recommendation: LOW priority.** Worth adopting when `ducknng` wants user-tunable
runtime knobs (e.g. default batch size, max frame size) to be settable through
`SET`/`RESET` rather than through a separate function call.

---

## `unstable_deprecated`

**Functions:** `duckdb_query_arrow`, `duckdb_arrow_array_scan`,
`duckdb_arrow_rows_changed`, etc.

These are the original Arrow result-set APIs, deprecated in favour of
`unstable_new_arrow_functions`.

**Recommendation: DO NOT USE.** Avoid entirely.

---

## `unstable_new_error_data_functions`

**Functions:** `duckdb_create_error_data`, `duckdb_error_data_message`,
`duckdb_error_data_has_error`

**What it does.** Richer structured error objects with type codes and messages.

**Recommendation: LOW priority.** Current string-based error propagation is adequate.
Adopt if `ducknng` exposes typed error codes through the protocol.

---

## `unstable_instance_cache`

**Functions:** `duckdb_create_instance_cache`, `duckdb_get_or_create_from_cache`

**What it does.** Shared database instance caching for multi-process use.

**Recommendation: NOT applicable.** `ducknng` manages its own database handle lifetime.

---

## `unstable_new_append_functions`

**Functions:** `duckdb_appender_create_query`, `duckdb_appender_error_data`,
`duckdb_appender_clear`

**What it does.** Extensions to the `duckdb_appender` API.

**Recommendation: NOT applicable.** `ducknng` does not use the appender API.

---

## Summary table

| Group | Priority | Action |
|---|---|---|
| `unstable_new_arrow_functions` | **HIGH** | Replace hand-written type switch in `ducknng_ipc_out.c` and `ducknng_sql_arrow.c` |
| `unstable_new_string_functions` / vector string | **MEDIUM** | Replace `ducknng_sql_bytes_look_text`; use `_len` assign in scan paths |
| `unstable_new_scalar_function_functions` | **MEDIUM** | Use bind phase for constant-argument scalar functions |
| `unstable_new_table_function_functions` | **LOW** | Simplify TVF registration by removing `extra_info` context pointer |
| `unstable_new_file_system_api` | **LOW** | Replace OS tempfile I/O in the Parquet branch |
| `unstable_new_expression_functions` | **LOW** | Pair with scalar bind phase work |
| `unstable_new_open_connect_functions` | **LOW** | Simplify session key lookup |
| `unstable_new_logger_functions` | **LOW** | Expose DuckDB log lines if needed |
| `unstable_new_config_options_functions` | **LOW** | Adopt when tunable knobs are wanted |
| `unstable_new_error_data_functions` | **LOW** | Adopt if typed error codes are surfaced |
| `unstable_new_copy_functions_api` | NOT applicable | — |
| `unstable_new_catalog_interface` | NOT applicable | — |
| `unstable_instance_cache` | NOT applicable | — |
| `unstable_new_append_functions` | NOT applicable | — |
| `unstable_deprecated` | **DO NOT USE** | Avoid entirely |
