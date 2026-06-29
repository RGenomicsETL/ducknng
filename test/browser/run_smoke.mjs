// Real-browser smoke test runner for the ducknng duckdb-wasm side module.
//
// The runner serves a staged duckdb-wasm site with COOP/COEP headers, drives the
// smoke page in headless Chromium through Playwright, and then runs explicit
// probes through the page's test API and SQL shell. Keep probes explicit: the
// default proves only extension load + one shell query. Transport probes must be
// requested by name.
//
// Usage:
//   node test/browser/run_smoke.mjs [siteDir]
//   node test/browser/run_smoke.mjs [siteDir] --probes=load,inproc,http-sync
//
// Environment:
//   DUCKNNG_BROWSER_PROBES=load,http-sync
//   BROWSER_DEBUG=1

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { resolve, extname, sep } from "node:path";
import { chromium } from "playwright";

const DEFAULT_SITE_DIR = ".duckdb-wasm-local-artifacts/site";
const SMOKE_PATH = "/scripts/duckdb-wasm-local-test.html";
const KNOWN_PROBES = new Set(["load", "inproc", "http-sync"]);

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".map": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
};

function parseArgs(argv) {
  let siteDir = null;
  let probes = process.env.DUCKNNG_BROWSER_PROBES || "load";

  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }
    if (arg.startsWith("--probes=")) {
      probes = arg.slice("--probes=".length);
      continue;
    }
    if (arg.startsWith("--site-dir=")) {
      siteDir = arg.slice("--site-dir=".length);
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`unknown option: ${arg}`);
    }
    if (siteDir) throw new Error(`unexpected extra argument: ${arg}`);
    siteDir = arg;
  }

  return {
    siteDir: resolve(siteDir || DEFAULT_SITE_DIR),
    probes: parseProbes(probes),
  };
}

function parseProbes(raw) {
  const probes = new Set();
  for (const item of String(raw || "load").split(",")) {
    const probe = item.trim();
    if (!probe) continue;
    if (probe === "baseline") {
      probes.add("load");
      probes.add("inproc");
      continue;
    }
    if (!KNOWN_PROBES.has(probe)) {
      throw new Error(`unknown browser probe: ${probe}`);
    }
    probes.add(probe);
  }
  probes.add("load");
  return probes;
}

function printUsage() {
  console.log(`Usage: node test/browser/run_smoke.mjs [siteDir] [--probes=load,inproc,http-sync]

Probes:
  load       Load DuckDB wasm, LOAD the ducknng extension, verify local
             COOP/COEP isolation, and run one SQL shell query. Enabled by
             default and required before all transport probes.
  inproc     Run the smoke page's built-in scalar/codec/inproc:// AIO proof.
             A false inproc result is accepted for non-threaded runtimes. For
             wasm_threads, a failure fails this diagnostic probe, but repeated
             headless runs are currently known to expose progress flakiness.
  http-sync  Exercise browser HTTP(S) sync client support through ducknng_ncurl
             against same-origin local GET/POST test endpoints plus an invalid
             headers_json error case. Enable only for artifacts expected to
             contain the browser HTTP bridge.

Alias:
  baseline   Expands to load,inproc.
`);
}

function setIsolationHeaders(res) {
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
  res.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  res.setHeader("Cache-Control", "no-store");
}

function readBody(req) {
  return new Promise((resolveBody) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolveBody(Buffer.concat(chunks)));
    req.on("error", () => resolveBody(Buffer.alloc(0)));
  });
}

async function handleProbe(req, res, pathname) {
  setIsolationHeaders(res);

  if (pathname === "/probe/hello" && req.method === "GET") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end('{"hello":"ducknng-http-ok"}');
    return;
  }

  if (pathname === "/probe/echo" && req.method === "POST") {
    const body = await readBody(req);
    res.setHeader("Content-Type", "application/octet-stream");
    res.end(body);
    return;
  }

  res.statusCode = 404;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end("probe not found");
}

function safeFilePath(root, pathname) {
  const decoded = decodeURIComponent(pathname);
  const relative = decoded === "/" ? "index.html" : decoded.replace(/^\/+/, "");
  const filePath = resolve(root, relative);
  if (filePath !== root && !filePath.startsWith(root + sep)) {
    throw new Error("path escapes site root");
  }
  return filePath;
}

function startServer(root) {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url || "/", "http://localhost");
      if (url.pathname.startsWith("/probe/")) {
        await handleProbe(req, res, url.pathname);
        return;
      }

      const filePath = safeFilePath(root, url.pathname);
      const info = await stat(filePath);
      if (!info.isFile()) throw new Error("not a file");
      const body = await readFile(filePath);

      setIsolationHeaders(res);
      res.setHeader("Content-Type", MIME[extname(filePath)] || "application/octet-stream");
      res.end(body);
    } catch {
      setIsolationHeaders(res);
      res.statusCode = 404;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("not found");
    }
  });

  return new Promise((resolveServer) => {
    server.listen(0, "127.0.0.1", () => {
      resolveServer({ server, port: server.address().port });
    });
  });
}

const RECORDED_FAILURE = "__ducknng_browser_smoke_failure_recorded__";

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
  throw new Error(RECORDED_FAILURE);
}

async function waitForSmokeApi(page) {
  await page.waitForFunction(() => !!globalThis.ducknngWasmSmoke?.setup, undefined, {
    timeout: 30000,
  });
}

async function runShell(page, sql, timeout = 60000) {
  await page.click("#clear-result");
  await page.fill("#sql", sql);
  await page.click("#run-sql");
  await page.waitForFunction(() => {
    const meta = document.getElementById("result-meta");
    const text = (meta && meta.textContent) || "";
    return text !== "No query has run yet." && /row|Error/i.test(text);
  }, undefined, { timeout });

  return await page.evaluate(() => ({
    meta: document.getElementById("result-meta")?.textContent || "",
    table: document.getElementById("result-table")?.textContent || "",
  }));
}

async function runLoadProbe(page) {
  const isolated = await page.evaluate(() => self.crossOriginIsolated === true);
  if (!isolated) fail("page is not crossOriginIsolated; local server did not apply COOP/COEP");
  console.log("ok: crossOriginIsolated");

  await page.evaluate(async () => await globalThis.ducknngWasmSmoke.setup());
  await page.waitForFunction(() => document.getElementById("run-sql")?.disabled === false, undefined, {
    timeout: 120000,
  });
  console.log("ok: extension loaded and SQL shell enabled");

  const shell = await runShell(page, "SELECT ducknng_nng_version() AS v");
  if (/error/i.test(shell.meta)) {
    fail(`shell query errored: ${shell.table}`);
  } else if (!/\d+\.\d+/.test(shell.table)) {
    fail(`shell query returned no version: "${shell.table}"`);
  } else {
    console.log(`ok: shell query returned a version (${shell.table.trim().slice(0, 40)})`);
  }
}

async function runInprocProbe(page) {
  const runtime = await page.evaluate(() => globalThis.ducknngWasmSmoke.runtimeConfig());
  const platform = runtime?.platform || "unknown";
  let result = false;

  try {
    result = await page.evaluate(async () => await globalThis.ducknngWasmSmoke.runSmokeTests());
  } catch (error) {
    fail(`inproc smoke threw for ${platform}: ${error?.message ?? String(error)}`);
  }

  if (result === true) {
    console.log(`ok: inproc smoke passed for ${platform}`);
    return;
  }
  if (platform !== "wasm_threads") {
    console.log(`ok: inproc smoke unavailable for ${platform}`);
    return;
  }
  fail("inproc smoke failed for wasm_threads");
}

async function runHttpSyncProbe(page, base) {
  const getSql =
    `SELECT ok, status, body_text FROM ducknng_ncurl('${base}/probe/hello', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const get = await runShell(page, getSql);
  if (/error/i.test(get.meta)) {
    fail(`http-sync GET errored: ${get.table}`);
  } else if (!/200/.test(get.table) || !/ducknng-http-ok/.test(get.table)) {
    fail(`http-sync GET did not return expected 200 body: "${get.table}"`);
  } else {
    console.log("ok: http-sync GET returned 200 + expected body");
  }

  const postSql =
    `SELECT ok, status, body_text FROM ducknng_ncurl('${base}/probe/echo', ` +
    `'POST', NULL, 'ducknng-post-roundtrip'::BLOB, 5000, 0::UBIGINT)`;
  const post = await runShell(page, postSql);
  if (/error/i.test(post.meta)) {
    fail(`http-sync POST errored: ${post.table}`);
  } else if (!/200/.test(post.table) || !/ducknng-post-roundtrip/.test(post.table)) {
    fail(`http-sync POST did not echo request body: "${post.table}"`);
  } else {
    console.log("ok: http-sync POST round-tripped the request body");
  }

  const badHeadersSql =
    `SELECT ok, error FROM ducknng_ncurl('${base}/probe/hello', ` +
    `'GET', '{"bad":true}', NULL, 5000, 0::UBIGINT)`;
  const badHeaders = await runShell(page, badHeadersSql);
  if (/error/i.test(badHeaders.meta)) {
    fail(`http-sync invalid headers_json raised a SQL error: ${badHeaders.table}`);
  } else if (!/false/i.test(badHeaders.table) || !/headers_json/i.test(badHeaders.table)) {
    fail(`http-sync invalid headers_json did not return the expected in-band error: "${badHeaders.table}"`);
  } else {
    console.log("ok: http-sync invalid headers_json returned an in-band error");
  }
}

let options;
try {
  options = parseArgs(process.argv.slice(2));
  await stat(options.siteDir).catch(() => {
    throw new Error(`site directory does not exist: ${options.siteDir}`);
  });
} catch (error) {
  console.error(`FAIL: ${error?.message ?? String(error)}`);
  printUsage();
  process.exit(1);
}

const { siteDir, probes } = options;
const { server, port } = await startServer(siteDir);
const base = `http://127.0.0.1:${port}`;
console.log(`serving ${siteDir} at ${base} (COOP/COEP enabled)`);
console.log(`browser probes: ${Array.from(probes).join(",")}`);

let browser = null;
try {
  browser = await chromium.launch({ headless: process.env.BROWSER_HEADFUL !== "1" });
  const page = await browser.newPage();
  page.on("pageerror", (error) => console.error(`[pageerror] ${error.message}`));
  if (process.env.BROWSER_DEBUG === "1") {
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
  }

  await page.goto(`${base}${SMOKE_PATH}?local_browser_smoke=${Date.now()}`, {
    waitUntil: "domcontentloaded",
    timeout: 30000,
  });
  await waitForSmokeApi(page);

  await runLoadProbe(page);
  if (probes.has("inproc")) await runInprocProbe(page);
  if (probes.has("http-sync")) await runHttpSyncProbe(page, base);
} catch (error) {
  if ((error?.message ?? String(error)) !== RECORDED_FAILURE) {
    console.error(`FAIL: ${error?.message ?? String(error)}`);
    process.exitCode = 1;
  }
} finally {
  if (browser) await browser.close();
  server.close();
}

console.log(process.exitCode === 1 ? "BROWSER SMOKE: FAIL" : "BROWSER SMOKE: PASS");
