# Browser support: a capability-driven design

This document is a design proposal, not a support claim. It complements the
running notes in [`docs/wasm.md`](wasm.md) and the review matrix in
[`docs/wasm_browser_transport_checklist.md`](wasm_browser_transport_checklist.md).
Those two files record *what has been proven, slice by slice*. This file argues
that the slice-by-slice method is itself the thing to fix, and sketches a more
generic structure that lets browser support grow by flipping capability flags
instead of threading a new `#ifdef __EMSCRIPTEN__` branch and a new hand-written
probe through every layer.

## The incrementalism we have today

Browser support has been built as a matrix of `runtime × transport × operation`
cells, each proven individually in a real browser and then written down. That
was the right way to *discover* what a browser runtime can do. It is a poor way
to *carry* the result, for four concrete reasons visible in the tree:

1. **`#ifdef __EMSCRIPTEN__` has leaked above the transport line.** The checklist
   asks to "keep the browser HTTP implementation behind the HTTP transport
   adapter boundary, not in the RPC method layer." In practice the browser fork
   now appears in `src/ducknng_http_compat.c` (`ducknng_http_transact`, the XHR
   branch at the top of the function), `src/ducknng_sql_aio.c` (a whole parallel
   `ducknng_client_add_terminal_http_*_aio` family), `src/ducknng_sql_socket.c`,
   and `src/ducknng_service.c`. Each new browser capability adds another guarded
   branch in another file rather than another binding behind one boundary.

2. **A synchronous XHR is the root of the async divergence.**
   `ducknng_wasm_http_fetch_perform` issues a blocking `XMLHttpRequest` via
   `EM_ASM`. It "is accepted for signature parity but ignored" for `timeout_ms`,
   cannot be cancelled, and blocks the worker for the duration of the request.
   Because the primitive is synchronous, the AIO surface cannot be real async in
   the browser, so `ducknng_ncurl_aio(...)` is implemented as a
   *terminal-at-launch* handle: the request has already completed by the time a
   handle exists (`ducknng_client_aio_alloc_terminal_slot` and its callers in
   `src/ducknng_sql_aio.c`). `ducknng_aio_status/wait/cancel` are then coherent
   only by construction, and every AIO-shaped function needs its own browser
   twin. This is one impedance mismatch, papered over once per operation.

3. **The pthread `inproc://` proof is a perpetual diagnostic.** Real effort keeps
   going into making `wasm_threads` `inproc://` progress reliably (COOP/COEP,
   the Pages COI service worker, `pthread_create` yielding, trace builds). The
   docs are honest that it "still expose[s] extension-load and inproc progress
   flakiness" and "is a diagnostic proof, not a stable regression gate yet." It
   may never become a gate on the current pthread-polling progress model, yet it
   keeps drawing incremental commits.

4. **Support is asserted in prose, not in a contract.** What the browser build
   can do is a growing English list ("Proven locally in real Chromium…"). There
   is no single machine-readable statement of capability that the extension
   reports and the test harness gates on. So proving one more cell means editing
   a table, adding a probe name to a `--probes=...` list, and writing another
   paragraph — for every cell, forever.

The through-line: the browser is treated as a set of exceptions to the native
code, discovered and pinned one at a time. The generic fix is to make the
browser one more *backend behind a single boundary*, described by a *capability
contract*, exercised by the *same* conformance suite native already runs.

## Design goals

- **One boundary.** No `#ifdef __EMSCRIPTEN__` above a single net-backend
  interface. The SQL method layer, the AIO layer, and the RPC/frame layer see
  the same API on native and in the browser.
- **One async primitive.** A single promise→AIO completion bridge, so async,
  timeout, and cancellation are implemented once and inherited by every
  AIO-shaped function instead of re-derived per function.
- **One carrier interface for frames.** RPC/session helpers speak "send frame,
  await reply frame" to a pluggable carrier, so a new carrier (browser
  WebSocket, later) is an added binding, not a new matrix column.
- **A capability contract, not prose.** The backend advertises what it supports;
  SQL can read it; the harness gates on it. Claiming a capability you cannot
  honor is a hard failure; not claiming one is a clean skip.
- **Research stays research.** The pthread `inproc://` path is explicitly a spike
  behind an `experimental` capability flag and is never a release gate on the
  current progress model.

## Proposed structure

### 1. A single net-backend interface

Introduce one internal vtable — call it `ducknng_net_backend` — with a small,
transport-agnostic surface:

```c
typedef struct ducknng_net_backend {
    const ducknng_net_caps *(*capabilities)(void);
    /* async by contract; native completes on NNG AIO, browser on the JS bridge */
    int (*http_submit)(const ducknng_http_request *req,
                       ducknng_completion *on_done);
    int (*frame_submit)(const ducknng_frame_request *req,
                        ducknng_completion *on_done);
    int (*cancel)(ducknng_op_handle h);
} ducknng_net_backend;
```

Native binds `http_submit`/`frame_submit` to the existing NNG HTTP client and
socket paths in `src/ducknng_http_compat.c`. The browser binds them to the JS
bridge (below). Selection happens once, at extension load, by target — not at
every call site. `ducknng_http_transact` and the `ducknng_ncurl*` SQL functions
call `backend->http_submit(...)`; the `__EMSCRIPTEN__` branch currently inside
`ducknng_http_transact` moves *into* the browser backend and disappears from the
shared code.

### 2. A capability descriptor the extension reports

Replace the prose matrix with a struct the backend fills in and SQL can read via
a new scalar (e.g. `ducknng_transport_capabilities()` returning JSON):

```c
typedef struct ducknng_net_caps {
    /* per-transport: unsupported | experimental | supported */
    ducknng_cap http;        /* http://  */
    ducknng_cap https;       /* https:// (TLS ownership below) */
    ducknng_cap inproc;      /* inproc:// */
    ducknng_cap tcp;         /* tcp:// */
    ducknng_cap ipc;         /* ipc:// */
    ducknng_cap tls_tcp;     /* tls+tcp:// */
    ducknng_cap websocket;   /* ws:// wss:// */
    /* async semantics */
    bool async_is_real;      /* false => terminal-at-launch handles */
    bool honors_timeout;
    bool honors_cancel;
    /* tls ownership */
    ducknng_tls_owner tls_owner; /* native | browser_managed */
} ducknng_net_caps;
```

The native backend reports everything `supported`, real async, native TLS. The
browser `wasm_eh` backend reports `http`/`https` supported, browser-managed TLS,
and (initially) `async_is_real = false`; everything else `unsupported`. The
`wasm_threads` `inproc://` spike sets `inproc = experimental`. The compatibility
table in `docs/wasm.md` becomes a *rendering* of this descriptor rather than a
source of truth maintained by hand.

### 3. One promise→AIO bridge (retire the terminal handles)

This is the change that removes the largest body of per-operation browser code.
Instead of a synchronous XHR that forces terminal-at-launch handles, implement a
single async bridge:

- JS side issues `fetch(url, { signal })` with an `AbortController`, and on
  settle pushes `{opId, status, headers, body}` (or an error) onto a completion
  queue.
- C side drains that queue on the extension's normal progress tick and completes
  the corresponding `nng_aio` / ducknng AIO object, exactly as the native NNG
  completion callback does today.
- `timeout_ms` maps to `AbortController.abort()` after the deadline; cancel maps
  to the same `abort()`. Both now work, so `honors_timeout`/`honors_cancel` flip
  to `true` and `async_is_real` becomes `true` for the browser backend.

With this in place, the entire `ducknng_client_add_terminal_http_*_aio` family in
`src/ducknng_sql_aio.c` collapses: `ducknng_ncurl_aio(...)`,
`ducknng_ncurl_table(...)`, and the framed helpers use the *same* AIO plumbing as
native, because the browser backend now delivers real completions. The
synchronous `EM_ASM` XHR in `src/ducknng_wasm_http_fetch.c` remains available
only as a fallback for runtimes without a usable event loop, gated by a
capability flag, not as the default path.

The bridge needs one of: Emscripten Asyncify around the submit call, or (leaner)
a completion-queue drain wired to the extension progress loop so no stack
unwinding is required. The queue-drain option keeps the C control flow identical
to native and is the recommended route; Asyncify is the fallback if a synchronous
SQL entry point must await inline.

### 4. A carrier interface for frames

The tree already contains "Use browser HTTP frame client fallback": RPC/session
frames are tunnelled over an HTTP POST when native sockets are unavailable.
Generalize that into an explicit **frame carrier** interface — "send this frame,
await the reply frame" — with implementations for native inproc/tcp, browser
HTTP POST, and, later, browser WebSocket. The RPC method layer in
`src/ducknng_sql_rpc.c` speaks only to the carrier, so:

- Browser `ws://`/`wss://` becomes "add a `WsFrameCarrier` that advertises
  `websocket = supported`," not a new column threaded through every RPC method.
- The frame wire format stays transport-independent, which is already the
  implicit contract the HTTP fallback relies on.

### 5. One conformance suite, gated by capabilities

Replace the growing `--probes=load,inproc,http-sync,http-aio,...` enumeration
with a single conformance suite (ideally the *same* SQL assertions native runs)
that reads `ducknng_transport_capabilities()` and:

- runs every assertion whose required capability is `supported`,
- **skips** assertions whose capability is `unsupported` (a clean, expected skip),
- runs `experimental` assertions in a non-gating lane,
- **fails hard** if a `supported` capability's assertion fails, or if an
  assertion succeeds for a capability the backend claims is `unsupported`
  (claim/behavior drift in either direction is a bug).

The Playwright runner under `test/browser/` then stops needing a bespoke probe
list per browser lane. Promoting a capability from `experimental` to `supported`
is a one-line descriptor change that immediately subjects it to the shared suite
— which is exactly the "flip a flag" property we want instead of "write another
slice."

## What this buys, and what it costs

**Buys:** browser support grows by editing a capability descriptor and binding a
backend method, not by forking each SQL function. Async/timeout/cancel are
correct once. `ws://` and any future carrier are additive. The support matrix
stops being hand-maintained prose. The pthread `inproc://` research is quarantined
behind `experimental` and no longer a source of gate flakiness.

**Costs:** the promise→AIO bridge is real work (a completion queue wired to the
progress loop, plus `AbortController` lifetime handling), and introducing the
`ducknng_net_backend` vtable is a refactor of live native code paths — it must be
behavior-preserving on native, verified by the existing native test suite before
any browser rebinding. The synchronous XHR shim should stay in-tree as the
capability-gated fallback during the transition rather than being deleted up
front.

## Suggested sequencing

1. Extract `ducknng_net_backend` and route native code through it with **no**
   behavior change; prove parity with the native test suite.
2. Add `ducknng_net_caps` + `ducknng_transport_capabilities()`; render the
   `docs/wasm.md` matrix from it.
3. Rebind the browser backend behind the same interface, still using synchronous
   XHR, reporting `async_is_real = false`. No behavior change versus today.
4. Land the promise→AIO bridge; flip the browser backend to real async and
   delete the terminal-handle twins in `src/ducknng_sql_aio.c`.
5. Introduce the frame-carrier interface; move the HTTP frame fallback behind it.
6. Convert the browser harness to the capability-gated conformance suite.
7. Only then consider a `WsFrameCarrier` and, separately, whether the pthread
   `inproc://` spike has a progress model worth promoting past `experimental`.

## Non-goals (unchanged)

This proposal does not change the browser sandbox facts recorded in the
checklist: raw `tcp://`, `ipc://`, and native `tls+tcp://` remain unsupported in
the browser; browser HTTPS TLS stays browser-managed and explicit ducknng TLS
config handles stay rejected rather than silently ignored; GitHub Pages wasm
artifacts remain demo provenance, not a stable binary release channel. The point
here is only to change *how support is structured and grown*, not to claim any
new transport.
