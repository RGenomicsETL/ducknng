from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

class NoCacheHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        if os.environ.get("DUCKNNG_WASM_COI") == "1":
            self.send_header("Cross-Origin-Opener-Policy", "same-origin")
            self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
            self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        super().end_headers()

if __name__ == "__main__":
    import os

    host = os.environ.get("DUCKNNG_WASM_HOST", "127.0.0.1")
    port = int(os.environ.get("DUCKNNG_WASM_PORT", "8002"))
    ThreadingHTTPServer((host, port), NoCacheHandler).serve_forever()
