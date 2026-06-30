.PHONY: clean clean_all check_news docs function_catalog rdm rpc_smoke rpc_smoke_r http_smoke subscriber_gateway_rdm

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

http_smoke: release
	python3 test/http_smoke.py build/release/ducknng.duckdb_extension

subscriber_gateway_rdm: release
	R -e "rmarkdown::render('demo/subscriber_gateway.Rmd')"

check_news:
ifdef BASE
	python3 scripts/check_news.py --base "$(BASE)"
else
	python3 scripts/check_news.py
endif

docs: rdm subscriber_gateway_rdm

rpc_smoke_r:
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript test/rpc_smoke.R; \
	else \
		echo "Rscript not found; skipping optional rpc_smoke_r"; \
	fi

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=ducknng
# USE_UNSTABLE_C_API=1 is required to access the DuckDB Arrow conversion API
# (duckdb_to_arrow_schema, duckdb_data_chunk_to_arrow, duckdb_schema_from_arrow,
# duckdb_data_chunk_from_arrow) and the error-data API (duckdb_create_error_data,
# duckdb_error_data_message, duckdb_error_data_has_error). These functions live
# behind DUCKDB_EXTENSION_API_VERSION_UNSTABLE in the extension vtable and are
# used in src/ducknng_ipc_out.c to replace ~530 lines of hand-written per-type
# Arrow encoding with correct, DuckDB-maintained conversions.
USE_UNSTABLE_C_API=1

# Target DuckDB version — must match DUCKDB_HEADER_VERSION so the unstable
# vtable layout seen at compile time matches the runtime vtable provided by the
# host.  Both the Python sqllogictest runner and the R duckdb package in use
# are v1.5.2, so all three version pins are kept in sync here.
TARGET_DUCKDB_VERSION=v1.5.0

# DuckDB version used by the Python sqllogictest runner — must match TARGET_DUCKDB_VERSION
DUCKDB_TEST_VERSION=1.5.0

# Actual DuckDB release to fetch headers from for compilation
DUCKDB_HEADER_VERSION=v1.5.0

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/$(DUCKDB_HEADER_VERSION)/src/include
DUCKDB_C_HEADER_URL=$(BASE_HEADER_URL)/duckdb.h
DUCKDB_C_EXTENSION_HEADER_URL=$(BASE_HEADER_URL)/duckdb_extension.h

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure

function_catalog:
	python3 function_catalog/generate_function_catalog.py

rdm: function_catalog
	R -e "rmarkdown::render('README.Rmd')"
