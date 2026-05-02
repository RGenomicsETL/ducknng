.PHONY: clean clean_all function_catalog rdm rpc_smoke rpc_smoke_r http_smoke motherduck_demo motherduck_rdm

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

http_smoke: release
	python3 test/http_smoke.py build/release/ducknng.duckdb_extension

motherduck_demo: release
	python3 demo/motherduck_gateway.py build/release/ducknng.duckdb_extension

motherduck_rdm: release
	R -e "rmarkdown::render('demo/motherduck_gateway.Rmd')"

rpc_smoke_r:
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript test/rpc_smoke.R; \
	else \
		echo "Rscript not found; skipping optional rpc_smoke_r"; \
	fi

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=ducknng
USE_UNSTABLE_C_API=0

# Stable C API metadata line
TARGET_DUCKDB_VERSION=v1.2.0

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
