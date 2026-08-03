# Native property tests

This directory contains C property tests for parser and protocol code that can
run without starting a DuckDB extension server. The runner uses vendored
[`greatest`](../vendor/greatest/LICENSE) for assertions and
[`theft`](../vendor/theft/LICENSE) for QuickCheck-style input generation and
shrinking.

Run the default bounded property suite with:

```sh
make prop
```

Useful variants:

```sh
make prop-quick          # fewer trials for rapid iteration
make prop-asan           # AddressSanitizer build of the same suite
make prop-ubsan          # UndefinedBehaviorSanitizer build
make prop-sanitize       # ASan then UBSan
make prop-clean
```

Environment/make knobs:

```sh
make prop PROP_TRIALS=10000
make prop PROP_SEED=0x1234
DUCKNNG_PROP_FORK=1 make prop
```

The suite currently targets byte-level ducknng frame decoding, transport URL
classification, and bounded `ducknng_quack_batch` row-count/scan-begin parsing.
These tests are structured property tests, not coverage-guided fuzzing;
libFuzzer or AFL++ harnesses would still be useful later for long-running
byte-stream fuzz campaigns.
