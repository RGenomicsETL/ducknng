self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  if (event.request.cache === "only-if-cached" && event.request.mode !== "same-origin") {
    return;
  }

  const url = new URL(event.request.url);
  if (url.origin === self.location.origin &&
      url.pathname.endsWith("/duckdb-wasm/ducknng.duckdb_extension.wasm")) {
    return;
  }

  event.respondWith((async () => {
    const response = await fetch(event.request);
    if (response.type === "opaque") {
      return response;
    }

    const headers = new Headers(response.headers);
    headers.set("Cross-Origin-Opener-Policy", "same-origin");
    headers.set("Cross-Origin-Embedder-Policy", "require-corp");
    if (new URL(event.request.url).origin === self.location.origin) {
      headers.set("Cross-Origin-Resource-Policy", "same-origin");
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  })());
});
