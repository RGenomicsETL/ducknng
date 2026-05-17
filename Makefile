.PHONY: clean clean_all check_news docs function_catalog rdm rpc_smoke rpc_smoke_r
.PHONY: rpc_bench rpc_bulk_compare http_smoke subscriber_gateway_rdm
.PHONY: prop prop-quick prop-regression prop-asan prop-ubsan prop-sanitize prop-clean

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

http_smoke: release
	python3 test/http_smoke.py build/release/ducknng.duckdb_extension

subscriber_gateway_rdm: release
	R -e "rmarkdown::render('demo/subscriber_gateway.Rmd')"

PROP_CC ?= cc
PROP_TRIALS ?= 1000
PROP_QUICK_TRIALS ?= 200
PROP_SEED ?= 0xd17c0ffee1234567
PROP_BIN := test/bin/ducknng_prop
PROP_ASAN_BIN := test/bin/ducknng_prop_asan
PROP_UBSAN_BIN := test/bin/ducknng_prop_ubsan
PROP_DUCKNNG_SRCS := \
	src/ducknng_wire.c \
	src/ducknng_transport.c \
	src/ducknng_util.c \
	src/ducknng_quack.c
PROP_THEFT_SRCS := $(wildcard test/vendor/theft/src/*.c)
PROP_SRCS := test/property/ducknng_prop.c $(PROP_DUCKNNG_SRCS) $(PROP_THEFT_SRCS)
PROP_HDRS := \
	$(wildcard test/vendor/greatest/*.h) \
	$(wildcard test/vendor/theft/inc/*.h) \
	$(wildcard test/vendor/theft/src/*.h) \
	$(wildcard src/include/*.h) \
	$(wildcard duckdb_capi/*.h)
PROP_COMMON_CFLAGS := \
	-std=c99 -g -O1 -Wall -Wextra -Wno-unused-function -D_DEFAULT_SOURCE \
	-DDUCKDB_EXTENSION_API_VERSION_MAJOR=1 \
	-DDUCKDB_EXTENSION_API_VERSION_MINOR=5 \
	-DDUCKDB_EXTENSION_API_VERSION_PATCH=2 \
	-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=v1.5.2 \
	-DTHEFT_USE_FLOATING_POINT=0 \
	-ffunction-sections -fdata-sections \
	-Itest/vendor/greatest \
	-Itest/vendor/theft/inc \
	-Itest/vendor/theft/src \
	-Isrc/include \
	-Iduckdb_capi \
	-Ithird_party/nng/include
PROP_COMMON_LDFLAGS := -Wl,--gc-sections -pthread

$(PROP_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) $(PROP_CFLAGS_EXTRA) $(PROP_SRCS) \
		$(PROP_COMMON_LDFLAGS) $(PROP_LDFLAGS_EXTRA) -o $@

$(PROP_ASAN_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) -O1 -fsanitize=address \
		-fno-omit-frame-pointer $(PROP_SRCS) $(PROP_COMMON_LDFLAGS) \
		-fsanitize=address -o $@

$(PROP_UBSAN_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) -O1 -fsanitize=undefined \
		-fno-omit-frame-pointer $(PROP_SRCS) $(PROP_COMMON_LDFLAGS) \
		-fsanitize=undefined -o $@

test/bin:
	mkdir -p test/bin

prop: $(PROP_BIN)
	DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) $(PROP_BIN)

prop-quick: $(PROP_BIN)
	DUCKNNG_PROP_TRIALS=$(PROP_QUICK_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) $(PROP_BIN)

prop-regression: prop

prop-asan: $(PROP_ASAN_BIN)
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
		DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) \
		$(PROP_ASAN_BIN)

prop-ubsan: $(PROP_UBSAN_BIN)
	UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1 \
		DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) \
		$(PROP_UBSAN_BIN)

prop-sanitize: prop-asan prop-ubsan

prop-clean:
	rm -f $(PROP_BIN) $(PROP_ASAN_BIN) $(PROP_UBSAN_BIN)

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

rpc_bench: release
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript bench/rpc_quack_bench.R; \
	else \
		echo "Rscript not found; skipping optional rpc_bench"; \
	fi

rpc_bulk_compare: release
	@if command -v R >/dev/null 2>&1; then \
		R -e "rmarkdown::render('bench/rpc_bulk_compare.Rmd')"; \
	else \
		echo "R not found; skipping optional rpc_bulk_compare"; \
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
TARGET_DUCKDB_VERSION=v1.5.2

# DuckDB version used by the Python sqllogictest runner — must match TARGET_DUCKDB_VERSION
DUCKDB_TEST_VERSION=1.5.2

# Actual DuckDB release to fetch headers from for compilation
DUCKDB_HEADER_VERSION=v1.5.2

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
