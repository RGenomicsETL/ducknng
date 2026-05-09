cmake_minimum_required(VERSION 3.16...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)

if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})
if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

project(${EXTENSION_NAME} LANGUAGES C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(USE_STATIC_MBEDTLS_LIBRARY ON CACHE BOOL "" FORCE)
set(USE_SHARED_MBEDTLS_LIBRARY OFF CACHE BOOL "" FORCE)
set(INSTALL_MBEDTLS_HEADERS OFF CACHE BOOL "" FORCE)
set(DISABLE_PACKAGE_CONFIG_AND_INSTALL ON CACHE BOOL "" FORCE)

add_subdirectory(third_party/mbedtls EXCLUDE_FROM_ALL)

set(NNG_DISABLE_PACKAGE_CONFIG_AND_INSTALL ON CACHE BOOL "" FORCE)
set(NNG_TESTS OFF CACHE BOOL "" FORCE)
set(NNG_TOOLS OFF CACHE BOOL "" FORCE)
set(NNG_ENABLE_HTTP ON CACHE BOOL "" FORCE)
set(NNG_ENABLE_TLS ON CACHE BOOL "" FORCE)
set(NNG_TLS_ENGINE "mbed" CACHE STRING "" FORCE)
set(NNG_TRANSPORT_IPC ON CACHE BOOL "" FORCE)
set(NNG_TRANSPORT_TCP ON CACHE BOOL "" FORCE)
set(NNG_TRANSPORT_TLS ON CACHE BOOL "" FORCE)
set(NNG_TRANSPORT_WS ON CACHE BOOL "" FORCE)
set(NNG_TRANSPORT_WSS ON CACHE BOOL "" FORCE)
set(NNG_PROTO_BUS0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PAIR0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PAIR1 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PUSH0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PULL0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PUB0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_SUB0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_REQ0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_REP0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_RESPONDENT0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_SURVEYOR0 ON CACHE BOOL "" FORCE)

add_subdirectory(third_party/nng EXCLUDE_FROM_ALL)

set(EXTENSION_SOURCES
    src/ducknng_extension.c
    src/ducknng_sql_api.c
    src/ducknng_sql_auth.c
    src/ducknng_sql_monitor.c
    src/ducknng_sql_service.c
    src/ducknng_sql_http.c
    src/ducknng_sql_tls.c
    src/ducknng_sql_socket.c
    src/ducknng_sql_aio.c
    src/ducknng_sql_registry.c
    src/ducknng_sql_session.c
    src/ducknng_sql_body.c
    src/ducknng_sql_arrow.c
    src/ducknng_sql_rpc.c
    src/ducknng_runtime.c
    src/ducknng_service.c
    src/ducknng_session.c
    src/ducknng_registry.c
    src/ducknng_methods.c
    src/ducknng_wire.c
    src/ducknng_ipc_in.c
    src/ducknng_ipc_out.c
    src/ducknng_transport.c
    src/ducknng_http_compat.c
    src/ducknng_nng_compat.c
    src/ducknng_util.c
    third_party/nanoarrow/src/nanoarrow/common/array.c
    third_party/nanoarrow/src/nanoarrow/common/schema.c
    third_party/nanoarrow/src/nanoarrow/common/array_stream.c
    third_party/nanoarrow/src/nanoarrow/common/utils.c
    third_party/nanoarrow/src/nanoarrow/ipc/codecs.c
    third_party/nanoarrow/src/nanoarrow/ipc/decoder.c
    third_party/nanoarrow/src/nanoarrow/ipc/encoder.c
    third_party/nanoarrow/src/nanoarrow/ipc/reader.c
    third_party/nanoarrow/src/nanoarrow/ipc/writer.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/builder.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/emitter.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/verifier.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/refmap.c
)

if (DUCKDB_WASM_EXTENSION)
    add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
else()
    add_library(${EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})
endif()

set_target_properties(${EXTENSION_NAME} PROPERTIES
    C_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")

target_include_directories(${EXTENSION_NAME} PRIVATE
    src/include
    duckdb_capi
    third_party/nng/include
    third_party/nanoarrow/src
    third_party/nanoarrow/thirdparty/flatcc/include
)

target_compile_definitions(${EXTENSION_NAME} PRIVATE
    NANOARROW_NAMESPACE=DucknngNanoarrow
)

target_link_libraries(${EXTENSION_NAME} PRIVATE nng Threads::Threads)
find_package(Threads REQUIRED)
