<!-- Landing page for the documentation site only. README.md remains the full
     GitHub-facing document; this page orients a first-time reader and hands
     off to the contracts. Rendered as index.html by scripts/build_docs_site.R
     with the table of contents disabled. -->

<div class="hero">
<p class="eyebrow">DuckDB extension &middot; pure C</p>
<h1 class="unlisted">DuckDB, on the network.</h1>
<p class="lede">
ducknng binds the <a href="https://nng.nanomsg.org/">NNG</a> scalability
protocols into DuckDB: framed RPC with Arrow and Quack payloads, query
sessions, mTLS and policy admission in C, and an HTTP carrier &mdash; so one
DuckDB session can serve or call another.
</p>
<p class="hero-actions">
<a class="button primary" href="#sec:get-started">Get started</a>
<a class="button" href="protocol.html">Read the protocol</a>
<a class="button" href="https://github.com/RGenomicsETL/ducknng">GitHub</a>
</p>
<p class="transport-strip">
<code>inproc://</code> <code>ipc://</code> <code>tcp://</code>
<code>tls+tcp://</code> <code>ws://</code> <code>wss://</code>
<code>http://</code> <code>https://</code>
</p>
</div>

<div class="diagram">
<svg viewBox="0 0 780 200" role="img" aria-label="Two DuckDB sessions exchanging a framed request and reply over a URL-selected transport">
  <defs>
    <marker id="dn-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" fill="#0b6b7a"/>
    </marker>
  </defs>
  <rect x="8" y="40" width="210" height="120" rx="12" fill="#ffffff" stroke="#d7dee6"/>
  <text x="113" y="72" text-anchor="middle" font-size="15" font-weight="700" fill="#063f49">DuckDB session</text>
  <text x="113" y="94" text-anchor="middle" font-size="12.5" fill="#5c6a76">client</text>
  <text x="113" y="126" text-anchor="middle" font-size="12" font-family="ui-monospace,monospace" fill="#0b6b7a">ducknng_query_rpc()</text>

  <rect x="562" y="40" width="210" height="120" rx="12" fill="#ffffff" stroke="#d7dee6"/>
  <text x="667" y="72" text-anchor="middle" font-size="15" font-weight="700" fill="#063f49">DuckDB session</text>
  <text x="667" y="94" text-anchor="middle" font-size="12.5" fill="#5c6a76">server</text>
  <text x="667" y="126" text-anchor="middle" font-size="12" font-family="ui-monospace,monospace" fill="#0b6b7a">ducknng_start_server()</text>

  <line x1="228" y1="82" x2="552" y2="82" stroke="#0b6b7a" stroke-width="2" marker-end="url(#dn-arrow)"/>
  <text x="390" y="72" text-anchor="middle" font-size="12.5" fill="#063f49">framed call &middot; Arrow or Quack</text>

  <line x1="552" y1="122" x2="228" y2="122" stroke="#6bb3bf" stroke-width="2" marker-end="url(#dn-arrow)"/>
  <text x="390" y="142" text-anchor="middle" font-size="12.5" fill="#5c6a76">result batches &middot; end-of-stream</text>

  <rect x="300" y="160" width="180" height="30" rx="15" fill="#f5f8fa" stroke="#d7dee6"/>
  <text x="390" y="180" text-anchor="middle" font-size="12" font-family="ui-monospace,monospace" fill="#063f49">tls+tcp://host:port</text>
</svg>
</div>

<div class="section-head">
<h2 id="sec:get-started">Get started</h2>
<span>Two DuckDB sessions, three steps, no broker in between.</span>
</div>

<div class="steps">

<div class="step">
<div class="step-head"><span class="step-num">1</span><h3 class="unlisted">Load the extension</h3></div>
<p>Build it first (see <a href="https://github.com/RGenomicsETL/ducknng#development">Development</a>), then load the artifact into any DuckDB session.</p>
<div class="step-body">

``` sql
LOAD 'build/release/ducknng.duckdb_extension';
```

</div>
</div>

<div class="step">
<div class="step-head"><span class="step-num">2</span><h3 class="unlisted">Serve a session over TLS</h3></div>
<p>Port <code>0</code> lets the OS assign a free port, and the self-signed certificate lives only in DuckDB memory &mdash; no files to manage.</p>
<div class="step-body">
<span class="code-label">Server session</span>

``` sql
SET VARIABLE tour_tls = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);

SELECT ducknng_start_server(
  'tour',                            -- service name
  'tls+tcp://127.0.0.1:0',           -- TLS; port 0 = OS-assigned
  1,                                 -- REP contexts
  134217728,                         -- recv_max_bytes (128 MiB)
  300000,                            -- session_idle_ms
  getvariable('tour_tls')::UBIGINT   -- TLS config handle
);

SELECT name, listen FROM ducknng_list_servers();
```

<table class="res">
<thead><tr><th>name</th><th>listen</th></tr></thead>
<tbody><tr><td>tour</td><td>tls+tcp://127.0.0.1:41573</td></tr></tbody>
</table>
</div>
</div>

<div class="step">
<div class="step-head"><span class="step-num">3</span><h3 class="unlisted">Query it from another session</h3></div>
<p>The URL scheme picks the carrier. The same call works over <code>ipc://</code>, <code>tcp://</code>, <code>wss://</code>, or <code>https://</code> without changing the method contract.</p>
<div class="step-cols">
<div>
<span class="code-label client">Client session &middot; remote rows</span>

``` sql
SELECT *
FROM ducknng_query_rpc(
  'ipc:///tmp/ducknng.ipc',
  'SELECT i, i > 10 AS gt_10
     FROM rpc_demo_t ORDER BY i',
  0::UBIGINT
);
```

<table class="res">
<thead><tr><th>i</th><th>gt_10</th></tr></thead>
<tbody>
<tr><td>7</td><td>false</td></tr>
<tr><td>11</td><td>true</td></tr>
<tr><td>42</td><td>true</td></tr>
</tbody>
</table>
</div>
<div>
<span class="code-label client">Client session &middot; discovery</span>

``` sql
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    getvariable('tour_url'),
    from_hex('0100000000000000000000
              00000000000000000000'),
    1000, getvariable('tour_tls')::UBIGINT
  )
);
```

<table class="res">
<thead><tr><th>ok</th><th>type_name</th><th>name</th></tr></thead>
<tbody><tr><td>true</td><td>result</td><td>manifest</td></tr></tbody>
</table>
</div>
</div>
</div>

</div>

<div class="demo-strip">
<div>
<h2 class="unlisted">Run it in your browser</h2>
<p>
The same extension compiles to a duckdb-wasm side module. The demo loads it in
your own tab, runs the smoke checks, and opens a SQL shell against that
connection &mdash; no server, no install.
</p>
<p class="hero-actions">
<a class="button primary" href="wasm/">Open the browser demo</a>
<a class="button" href="browser.html">Support contract</a>
</p>
</div>
<div>

``` sql
SELECT provider, media_types
FROM ducknng_list_codecs()
ORDER BY provider
LIMIT 3;
```

<table class="res">
<thead><tr><th>provider</th><th>media_types</th></tr></thead>
<tbody>
<tr><td>arrow</td><td>application/vnd.apache.arrow.stream</td></tr>
<tr><td>ducknng</td><td>application/vnd.ducknng.frame</td></tr>
<tr><td>json</td><td>application/json</td></tr>
</tbody>
</table>
</div>
</div>

<div class="section-head">
<h2 id="sec:layers">How it is layered</h2>
<span>Each layer is separately contracted, so the one below can change without the one above.</span>
</div>

<div class="layer-stack">
<div class="layer"><strong>Transport</strong><span>NNG sockets and listeners. Synchronous send and receive, one-shot AIO handles, pipe-event telemetry.</span></div>
<div class="layer"><strong>Framed RPC</strong><span>A versioned envelope carrying Arrow IPC or JSON control text, with an explicit status byte. HTTP and HTTPS mount the same methods at a URL path.</span></div>
<div class="layer"><strong>Policy</strong><span>Fast C admission: mTLS, exact peer-identity allowlists, IP and CIDR allowlists, per-service and per-principal limits, optional SQL authorizer.</span></div>
<div class="layer"><strong>Codecs</strong><span>Content-type-tagged bodies: JSON, Arrow IPC, ducknng frames, the Quack batch media type, and text or raw fallback. User codecs extend the set.</span></div>
</div>

<div class="section-head">
<h2 id="sec:where-next">Where next</h2>
<span>Eight published contracts. Everything else lives in the repository.</span>
</div>

<div class="next-grid">
<a href="protocol.html">Protocol<span>Envelope, statuses, session lifecycle</span></a>
<a href="reference.html">Reference<span>Every SQL function</span></a>
<a href="types.html">Types<span>What crosses the wire</span></a>
<a href="transports.html">Transports<span>Choosing a carrier by URL</span></a>
<a href="http.html">HTTP<span>The HTTP and HTTPS binding</span></a>
<a href="browser.html">Browser<span>duckdb-wasm client scope</span></a>
<a href="security.html">Security<span>Admission and the trust boundary</span></a>
<a href="wasm/">Demo<span>Run ducknng in a tab</span></a>
</div>

<div class="callout">
<strong>Status.</strong> ducknng is pre-1.0 and the SQL surface is still
sealing. The protocol envelope, supported types, and lifetime rules in the
pages above are binding; anything not written there is subject to change.
</div>
