#include "ducknng_net_backend.h"
#include "ducknng_http_compat.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Build-target net backend selection (docs/browser_support.md, issue #8).
 * One selection at build time; shared code reads ducknng_net_backend_get()
 * and never forks on __EMSCRIPTEN__ itself.
 *
 * The capability descriptors for every build target are plain data compiled
 * into all targets, so the docs/wasm.md matrix and the conformance harness
 * can be rendered from ducknng_net_caps_all() instead of hand-maintained
 * prose; only the active backend selection is target-conditional.
 */

static const ducknng_net_caps ducknng_all_caps[] = {
    {
        .backend_name = "native",
        .http = DUCKNNG_NET_CAP_SUPPORTED,
        .https = DUCKNNG_NET_CAP_SUPPORTED,
        .inproc = DUCKNNG_NET_CAP_SUPPORTED,
        .tcp = DUCKNNG_NET_CAP_SUPPORTED,
        .ipc = DUCKNNG_NET_CAP_SUPPORTED,
        .tls_tcp = DUCKNNG_NET_CAP_SUPPORTED,
        .websocket = DUCKNNG_NET_CAP_SUPPORTED,
        .async_is_real = 1,
        .honors_timeout = 1,
        .honors_cancel = 1,
        .tls_owner = DUCKNNG_NET_TLS_OWNER_NATIVE,
    },
    {
        .backend_name = "wasm_eh",
        .http = DUCKNNG_NET_CAP_SUPPORTED,
        .https = DUCKNNG_NET_CAP_SUPPORTED,
        .inproc = DUCKNNG_NET_CAP_UNSUPPORTED, /* no pthread worker for NNG progress */
        .tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
        .ipc = DUCKNNG_NET_CAP_UNSUPPORTED,
        .tls_tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
        .websocket = DUCKNNG_NET_CAP_UNSUPPORTED,
        /* AIO rides the fetch completion-queue bridge: handles stay pending
         * until the worker event loop settles them, timeouts arm a JS timer,
         * and cancel aborts the controller. The synchronous ncurl path still
         * uses blocking XHR and ignores timeout_ms. */
        .async_is_real = 1,
        .honors_timeout = 1,
        .honors_cancel = 1,
        .tls_owner = DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED,
    },
    {
        .backend_name = "wasm_threads",
        .http = DUCKNNG_NET_CAP_SUPPORTED,
        .https = DUCKNNG_NET_CAP_SUPPORTED,
        .inproc = DUCKNNG_NET_CAP_EXPERIMENTAL, /* pthread progress spike; not a gate */
        .tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
        .ipc = DUCKNNG_NET_CAP_UNSUPPORTED,
        .tls_tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
        .websocket = DUCKNNG_NET_CAP_UNSUPPORTED,
        .async_is_real = 1,
        .honors_timeout = 1,
        .honors_cancel = 1,
        .tls_owner = DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED,
    },
};

const ducknng_net_caps *ducknng_net_caps_all(size_t *out_count) {
    if (out_count) *out_count = sizeof(ducknng_all_caps) / sizeof(ducknng_all_caps[0]);
    return ducknng_all_caps;
}

#if defined(__EMSCRIPTEN__) && defined(__EMSCRIPTEN_PTHREADS__)
#define DUCKNNG_ACTIVE_CAPS_INDEX 2 /* wasm_threads */
#elif defined(__EMSCRIPTEN__)
#define DUCKNNG_ACTIVE_CAPS_INDEX 1 /* wasm_eh */
#else
#define DUCKNNG_ACTIVE_CAPS_INDEX 0 /* native */
#endif

static const ducknng_net_caps *ducknng_active_capabilities(void) {
    return &ducknng_all_caps[DUCKNNG_ACTIVE_CAPS_INDEX];
}

#ifdef __EMSCRIPTEN__
static const ducknng_net_backend ducknng_active_backend = {
    .capabilities = ducknng_active_capabilities,
    .http_transact = ducknng_http_transact_browser,
    .frame_client_open = ducknng_http_frame_client_open_browser,
    .frame_client_transact = ducknng_http_frame_client_transact_browser,
    .frame_client_transact_msg = ducknng_http_frame_client_transact_msg_browser,
};
#else
static const ducknng_net_backend ducknng_active_backend = {
    .capabilities = ducknng_active_capabilities,
    .http_transact = ducknng_http_transact_native,
    .frame_client_open = ducknng_http_frame_client_open_native,
    .frame_client_transact = ducknng_http_frame_client_transact_native,
    .frame_client_transact_msg = ducknng_http_frame_client_transact_msg_native,
};
#endif

const ducknng_net_backend *ducknng_net_backend_get(void) {
    return &ducknng_active_backend;
}

const char *ducknng_net_cap_name(ducknng_net_cap cap) {
    switch (cap) {
    case DUCKNNG_NET_CAP_SUPPORTED: return "supported";
    case DUCKNNG_NET_CAP_EXPERIMENTAL: return "experimental";
    default: return "unsupported";
    }
}

const char *ducknng_net_tls_owner_name(ducknng_net_tls_owner owner) {
    return owner == DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED ? "browser_managed" : "native";
}

char *ducknng_net_caps_to_json(const ducknng_net_caps *caps) {
    char buf[512];
    int written;

    if (!caps) return NULL;
    written = snprintf(buf, sizeof(buf),
        "{\"backend\":\"%s\","
        "\"http\":\"%s\",\"https\":\"%s\",\"inproc\":\"%s\",\"tcp\":\"%s\","
        "\"ipc\":\"%s\",\"tls_tcp\":\"%s\",\"websocket\":\"%s\","
        "\"async_is_real\":%s,\"honors_timeout\":%s,\"honors_cancel\":%s,"
        "\"tls_owner\":\"%s\"}",
        caps->backend_name ? caps->backend_name : "unknown",
        ducknng_net_cap_name(caps->http),
        ducknng_net_cap_name(caps->https),
        ducknng_net_cap_name(caps->inproc),
        ducknng_net_cap_name(caps->tcp),
        ducknng_net_cap_name(caps->ipc),
        ducknng_net_cap_name(caps->tls_tcp),
        ducknng_net_cap_name(caps->websocket),
        caps->async_is_real ? "true" : "false",
        caps->honors_timeout ? "true" : "false",
        caps->honors_cancel ? "true" : "false",
        ducknng_net_tls_owner_name(caps->tls_owner));
    if (written < 0 || (size_t)written >= sizeof(buf)) return NULL;
    return ducknng_strdup(buf);
}
