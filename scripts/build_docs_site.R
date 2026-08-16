#!/usr/bin/env Rscript

# Builds the curated ducknng documentation site into _site/ with litedown.
# The site publishes a deliberately small subset of docs/: the contracts a
# user needs to talk to a ducknng service. Everything else under docs/ stays
# in-repo for contributors rather than becoming a public page.

if (!requireNamespace("litedown", quietly = TRUE)) {
  stop("Package 'litedown' is required to build the documentation site", call. = FALSE)
}

pages <- c(
  index = "README.md",
  protocol = "docs/protocol.md",
  reference = "docs/function_reference.md",
  types = "docs/types.md",
  transports = "docs/transports.md",
  http = "docs/http.md",
  browser = "docs/browser_support.md",
  security = "docs/security.md"
)
titles <- c(
  index = "ducknng",
  protocol = "Protocol specification · ducknng",
  reference = "SQL function reference · ducknng",
  types = "Supported types · ducknng",
  transports = "Transports · ducknng",
  http = "HTTP carrier · ducknng",
  browser = "Browser support · ducknng",
  security = "Security model · ducknng"
)

missing_sources <- pages[!file.exists(pages)]
if (length(missing_sources) > 0L) {
  stop(
    "Curated site sources are missing: ",
    paste(sprintf("%s (%s)", names(missing_sources), missing_sources), collapse = ", "),
    call. = FALSE
  )
}

site_dir <- "_site"
unlink(site_dir, recursive = TRUE, force = TRUE)
dir.create(site_dir, recursive = TRUE, showWarnings = FALSE)
invisible(file.create(file.path(site_dir, ".nojekyll")))

css <- normalizePath("tools/site.css", winslash = "/", mustWork = TRUE)
header <- normalizePath("tools/site-header.html", winslash = "/", mustWork = TRUE)

build_metadata <- function(include_before) {
  c(
    "---",
    "output:",
    "  html:",
    "    options:",
    "      toc: true",
    "    meta:",
    paste0(
      "      css: [\"@default@1.14.69\", \"@article@1.14.69\", ",
      "\"@site@1.14.69\", \"", css, "\"]"
    ),
    paste0("      include_before: \"", include_before, "\""),
    "---"
  )
}

# The landing page leads with a hero and a card grid instead of the README's
# plain heading. This lives here rather than in README.md so the GitHub view of
# the README stays clean markdown. It is appended to the nav header and injected
# through include_before, which places it above litedown's table of contents;
# passing it as markdown put the whole TOC ahead of the hero.
hero <- c(
  "<div class=\"hero-wrap\">",
  "<div class=\"hero\">",
  "<p class=\"eyebrow\">DuckDB extension &middot; pure C</p>",
  "<h1>DuckDB, on the network.</h1>",
  "<p class=\"lede\">ducknng binds the <a href=\"https://nng.nanomsg.org/\">NNG</a>",
  "scalability protocols into DuckDB: framed RPC with Arrow and Quack payloads,",
  "query sessions, mTLS and policy admission in C, and an HTTP carrier &mdash;",
  "so one DuckDB session can serve or call another.</p>",
  "<p class=\"hero-actions\">",
  "<a class=\"button primary\" href=\"protocol.html\">Read the protocol</a>",
  "<a class=\"button\" href=\"reference.html\">SQL reference</a>",
  "<a class=\"button\" href=\"https://github.com/RGenomicsETL/ducknng\">GitHub</a>",
  "</p>",
  "<p class=\"transport-strip\">",
  paste0(
    "<code>inproc://</code> <code>ipc://</code> <code>tcp://</code> ",
    "<code>tls+tcp://</code> <code>ws://</code> <code>wss://</code> ",
    "<code>http://</code> <code>https://</code>"
  ),
  "</p>",
  "</div>",
  "",
  "<div class=\"feature-grid\">",
  "<a class=\"feature\" href=\"protocol.html\">",
  "<strong>Framed RPC</strong>",
  "<span>A small versioned envelope carrying Arrow IPC or JSON, with an",
  "explicit status byte and a fixed four-method query session.</span></a>",
  "<a class=\"feature\" href=\"reference.html\">",
  "<strong>SQL surface</strong>",
  "<span>Servers, sockets, AIO futures, TLS configs, codecs, and HTTP routes,",
  "all managed from SQL as explicit handles.</span></a>",
  "<a class=\"feature\" href=\"types.html\">",
  "<strong>Quack payloads</strong>",
  "<span>DuckDB's own BinarySerializer batch format alongside Arrow IPC,",
  "carrying nested types and compressed vectors.</span></a>",
  "<a class=\"feature\" href=\"security.html\">",
  "<strong>Admission in C</strong>",
  "<span>mTLS, exact peer-identity and CIDR allowlists, per-principal limits,",
  "and an optional SQL authorizer at the request boundary.</span></a>",
  "<a class=\"feature\" href=\"transports.html\">",
  "<strong>Transport by URL</strong>",
  "<span>The scheme picks the carrier. The method contract does not change",
  "between inproc, IPC, TCP, TLS, WebSocket, and HTTP.</span></a>",
  "<a class=\"feature\" href=\"browser.html\">",
  "<strong>Browser clients</strong>",
  "<span>A duckdb-wasm side module speaking the same protocol over HTTPS and",
  "WSS, with capability negotiation rather than simulation.</span></a>",
  "</div>",
  "</div>"
)

# The index gets the nav plus the hero; every other page gets the nav alone.
index_header <- file.path(tempdir(), "ducknng-site-index-header.html")
writeLines(
  c(readLines(header, warn = FALSE, encoding = "UTF-8"), hero),
  index_header
)

read_page <- function(name, source) {
  markdown <- readLines(source, warn = FALSE, encoding = "UTF-8")
  if (name != "index") {
    return(markdown)
  }
  # Drop the duckknit provenance comment and the leading "# ducknng" heading;
  # the hero supplies the title on this page.
  markdown <- markdown[!grepl("^<!-- README\\.md is generated", markdown)]
  first_heading <- which(grepl("^# ", markdown))[1]
  if (!is.na(first_heading)) {
    markdown <- markdown[-first_heading]
  }
  while (length(markdown) > 0L && !nzchar(trimws(markdown[1]))) {
    markdown <- markdown[-1]
  }
  markdown
}

for (name in names(pages)) {
  source <- pages[[name]]
  destination <- file.path(site_dir, paste0(name, ".html"))
  include_before <- if (name == "index") index_header else header
  message("rendering ", source, " -> ", destination)
  litedown::mark(
    text = c(build_metadata(include_before), read_page(name, source)),
    output = destination,
    meta = list("plain-title" = titles[[name]])
  )
}

required <- file.path(site_dir, paste0(names(pages), ".html"))
missing <- required[!file.exists(required) | file.info(required)$size == 0]
if (length(missing) > 0L) {
  stop("Site build did not produce: ", paste(missing, collapse = ", "), call. = FALSE)
}

message("built ", length(required), " pages into ", site_dir, "/")
