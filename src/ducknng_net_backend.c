#include "ducknng_net_backend.h"
#include "ducknng_http_compat.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Build-target net backend selection (docs/browser_support.md, issue #8).
 * One selection at build time; shared code reads ducknng_net_backend_get()
 * and never forks on __EMSCRIPTEN__ itself. The capability descriptor is the
 * machine-readable version of the docs/wasm.md support matrix.
 */

#ifdef __EMSCRIPTEN__

static const ducknng_net_caps ducknng_browser_caps = {
    .backend_name = "browser",
    .http = DUCKNNG_NET_CAP_SUPPORTED,
    .https = DUCKNNG_NET_CAP_SUPPORTED,
#ifdef __EMSCRIPTEN_PTHREADS__
    .inproc = DUCKNNG_NET_CAP_EXPERIMENTAL,
#else
    .inproc = DUCKNNG_NET_CAP_UNSUPPORTED,
#endif
    .tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
    .ipc = DUCKNNG_NET_CAP_UNSUPPORTED,
    .tls_tcp = DUCKNNG_NET_CAP_UNSUPPORTED,
    .websocket = DUCKNNG_NET_CAP_UNSUPPORTED,
    .async_is_real = 0,   /* AIO handles are terminal at launch (sync XHR) */
    .honors_timeout = 0,  /* synchronous XHR ignores timeout_ms */
    .honors_cancel = 0,
    .tls_owner = DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED,
};

static const ducknng_net_caps *ducknng_browser_capabilities(void) {
    return &ducknng_browser_caps;
}

static const ducknng_net_backend ducknng_browser_backend = {
    .capabilities = ducknng_browser_capabilities,
    .http_transact = ducknng_http_transact_browser,
};

const ducknng_net_backend *ducknng_net_backend_get(void) {
    return &ducknng_browser_backend;
}

#else /* native */

static const ducknng_net_caps ducknng_native_caps = {
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
};

static const ducknng_net_caps *ducknng_native_capabilities(void) {
    return &ducknng_native_caps;
}

static const ducknng_net_backend ducknng_native_backend = {
    .capabilities = ducknng_native_capabilities,
    .http_transact = ducknng_http_transact_native,
};

const ducknng_net_backend *ducknng_net_backend_get(void) {
    return &ducknng_native_backend;
}

#endif /* __EMSCRIPTEN__ */

static const char *ducknng_net_cap_name(ducknng_net_cap cap) {
    switch (cap) {
    case DUCKNNG_NET_CAP_SUPPORTED: return "supported";
    case DUCKNNG_NET_CAP_EXPERIMENTAL: return "experimental";
    default: return "unsupported";
    }
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
        caps->tls_owner == DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED ?
            "browser_managed" : "native");
    if (written < 0 || (size_t)written >= sizeof(buf)) return NULL;
    return ducknng_strdup(buf);
}
