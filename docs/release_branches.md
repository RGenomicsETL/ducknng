# Release branches and self-published binaries

`ducknng` uses `main` for forward development. Because the extension uses DuckDB's unstable C extension vtable, each built `.duckdb_extension` is pinned to one exact DuckDB version. A fix that downstreams need on an older DuckDB version must therefore be backported to a branch named for that DuckDB runtime, for example `release/duckdb-1.5.2`.

A release branch pins three things together in `Makefile`: `TARGET_DUCKDB_VERSION`, `DUCKDB_TEST_VERSION`, and `DUCKDB_HEADER_VERSION`. The checked-in `duckdb_capi/duckdb.h` and `duckdb_capi/duckdb_extension.h` must come from the same DuckDB tag. Do not change one of these without the others.

The backport workflow is: land the fix on `main`, cherry-pick it to every supported `release/duckdb-X.Y.Z` branch, update the version pins and C API headers for that branch if the branch is newly created, then build and test with the matching DuckDB runtime:

```sh
make configure
make update_duckdb_headers
make release -j2
configure/venv/bin/python3 test/http_smoke.py build/release/ducknng.duckdb_extension
```

The DuckDB community extension repository is not the backport distribution channel. It builds the community submission line and does not carry per-DuckDB-version fixes. Backport binaries are published from this repository as unsigned GitHub Release assets. Tag release-branch commits with a tag that pins both versions, using:

```text
v<ducknng-version>+duckdb<duckdb-version>
```

For example, `v0.1.1+duckdb1.5.2` names the `ducknng` 0.1.1 backport line built for DuckDB 1.5.2. Pushing such a tag runs `.github/workflows/ducknng-release-binaries.yml`, builds the branch's pinned Linux extension, runs the HTTP smoke test, and attaches the `.duckdb_extension`, the raw shared library, checksums, and a short load note to the GitHub Release.

These release assets are unsigned. A consumer must open the host DuckDB connection with `allow_unsigned_extensions = true` and then load the downloaded file explicitly:

```sql
LOAD '/path/to/ducknng-v0.1.1+duckdb1.5.2-linux_amd64.duckdb_extension';
```

Community-signed builds remain loadable through the usual community extension flow when the consumer can use the community line's DuckDB target. The self-published assets are for pinned runtimes that need a backport before, or instead of, a community submission refresh.
