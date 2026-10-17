# llm-token-udf

Companion code for the "Executable UDFs are now generally available on ClickHouse Cloud" post.
Token accounting and quality evals for LLM traces, done inside ClickHouse with two executable UDFs.

```
llm-token-udf/
├── udf/
│   ├── count_tokens_rs/   # Native runtime UDF (Rust, tiktoken-rs). The one used in the post.
│   ├── count_token_chrs/  # Parallel Rust UDF using clickhouse::udf::Executor.
│   ├── count_tokens/      # Same UDF in Go (pure-Go tokenizer). Kept for the language comparison.
│   ├── count_tokens_py/   # Same UDF in Python (tiktoken). Kept for the language comparison.
│   └── judge_response/    # Network-enabled Python UDF: LLM-as-judge via Anthropic tool-use.
├── sql/
│   ├── 01_schema.sql      # llm_spans source table
│   ├── 02_token_mv.sql    # llm_span_tokens + the MV that calls count_tokens() at insert time
│   ├── 03_prices.sql      # model_prices via url() + refreshable MV + dictionary
│   ├── 04_evals.sql       # llm_evals + refreshable MV that samples and judges completions
│   └── 05_queries.sql     # cost attribution, caching savings, context bloat, ProfileEvents, ...
├── terraform/
│   └── main.tf            # clickhouse_udf + dev (follows latest) / prod (pinned) attachments
└── local/                 # XML function config for running the same UDFs against OSS ClickHouse
```

## count_tokens (Native runtime, Rust)

`count_tokens(model String, text String) -> UInt32`

Build static binaries for both architectures and package them:

```bash
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
cd udf/count_tokens_rs && ./build.sh      # -> count_tokens.zip
```

Cloud settings: type `executable_pool`, runtime `Native`, format `RowBinary`, send chunk header on,
deterministic on, pool size 4. Arguments `model String`, `text String`; return type `UInt32`.

## judge_response (Python, network access)

`judge_response(prompt String, completion String) -> Tuple(UInt8, String, String)`

```bash
cd udf/judge_response
cp config.example.json config.json       # put your Anthropic API key in here
zip judge_response.zip main.py requirements.txt config.json
```

Cloud settings: type `executable_pool`, runtime `python3.11`, format `JSONEachRow`, network access on,
pool size 4, max command execution time 30s. Arguments `prompt String`, `completion String`;
return type `Tuple(UInt8, String, String)`.

## Running the SQL

```
:run sql/01_schema.sql
:run sql/02_token_mv.sql
:run sql/03_prices.sql
:run sql/04_evals.sql
```

Then insert into `llm_spans` and query `llm_span_tokens`, `llm_evals`, and `system.query_log`
as in `sql/05_queries.sql`.

## Testing locally without Cloud

Everything here also runs against open-source ClickHouse via an XML function config, which is how
the numbers in the post were produced. `local/udf_functions.xml` has the function definitions and
the steps.

### Executor regression and benchmarks

`count_token_chrs(model String, text String) -> UInt32` implements the same tokenizer behavior
using `clickhouse::udf::Executor` from the
[`add-public-unvalidated-rowbinary-serde-helpers` branch](https://github.com/theelderbeever/clickhouse-rs/tree/add-public-unvalidated-rowbinary-serde-helpers),
pinned to `e350b48f45bea062ac1af152d54698b1b7aee568`. The existing `count_tokens_rs` source and lockfile
are the reference. Both use its `models.json`; the runner stages that file next to both binaries.

Install native [ClickHouse](https://clickhouse.com/docs/install), Rust, and `uv`, then run:

```bash
uv run local/regression.py                       # routine: 5 measured repetitions, 1 and 4 workers
uv run local/regression.py --profile quick       # smaller workload, 2 repetitions, 1 worker
uv run local/regression.py --correctness-only    # full correctness corpus, no benchmarks
uv run local/regression.py --profile stress      # larger inputs, 1/2/4 workers, two block sizes
```

The runner builds both binaries in release mode with locked dependencies. It starts its own native
ClickHouse servers on available loopback HTTP ports, with separate configuration and data. It does
not connect to an existing server. Each server runs only the implementation under measurement;
its processes are stopped on completion or failure. A fresh pool provides a separate cold-start
measurement. Both encodings and all workers are warmed before five routine measured repetitions.

Direct protocol tests and ClickHouse queries compare every output, including empty text, all model
rules, unknown models, Unicode, arbitrary bytes, fragmented input, pooled reuse, and a row larger
than 1 MiB. Error tests cover missing/invalid configuration and truncated input. ClickHouse always
sends decimal chunk counts followed by newline: Executor intentionally rejects whitespace, signed
headers, and unterminated headers that the reference's permissive parser can accept. These malformed
header differences are not token-count regressions.

Routine workloads contain 10,000 short texts (approximately 256 bytes), 1,000 medium texts (4 KiB),
and 128 long texts (64 KiB), with one or four distinct UDF calls per row. Each query uses one server
thread; query concurrency equals the configured pool size (1 or 4). Routine block sizes are 256
for one worker and 8192 for four workers. Stress tests exercise both block sizes at each concurrency.
The query cache is disabled. Results are aggregated to keep client transfer cost small; row-by-row
correctness runs separately. Timing includes local HTTP overhead, and concurrent queries each scan
the full workload, so use throughput when comparing worker counts.

Results and server logs are saved under `local/results/<timestamp>/` (gitignored). `results.json`
retains raw query logs, checksums, query IDs, machine/compiler/server versions, dependency revision,
and input/binary hashes. `results.csv` contains one measurement per query batch. Measurements include
client wall time, query duration, throughput, invocations, pool wait, UDF CPU, pipe bytes, sampled
worker RSS, and separate server memory. Source rows, UDF input/output values, and aggregate result
rows have separate columns. Unsupported or unattributed counters are `null`, not zero.
`comparison.csv` and `comparison.json` group the warm repetitions by workload, calls per row,
worker count, and block size. They report each metric's median/range and Executor/baseline ratio.
Ratios below one favor Executor for time and memory; ratios above one favor it for throughput.

Worker RSS is sampled every 20 ms, sums all pool workers (including idle workers), and can miss brief
peaks or double-count shared pages. Sampled worker CPU and average cores are recorded alongside
ClickHouse's UDF CPU counters. The integrated peak-memory byte-seconds event is separate from RSS.
Correctness failures and timeouts fail the command; performance differences are report-only.

Options: `--clickhouse /path/to/clickhouse`, `--output-dir /new/results/path`, `--seed 1729`, and
`--skip-build` (reuse existing release binaries). The output directory must not already exist.

To produce a separate Cloud Native bundle, install both Linux musl targets and the appropriate
cross-linkers, then run `cd udf/count_token_chrs && ./build.sh`. It creates `count_token_chrs.zip`
with `amd64/main`, `arm64/main`, and their adjacent configuration. Cross-compilation is independent
of the native regression runner. For manual OSS deployment, copy the new binary as
`count_token_chrs` alongside the baseline binary and `models.json` in `user_scripts_path`.

One thing worth knowing if you adapt the binaries: data files in the zip are deployed next to the
binary, but the process does not start in that directory (in Cloud the bundle is under `/scripts`
and the working directory is `/`). Both `count_tokens` implementations resolve `models.json`
relative to the executable for that reason, and exit with an error if it is missing rather than
silently falling back.

## Prebuilt binaries

`udf/count_tokens_rs/count_tokens.zip` is committed so you can upload it without a Rust toolchain
(statically linked, linux/amd64 + linux/arm64). Rebuild with `build.sh` if you change the source.

## License

Apache 2.0, see `LICENSE`.
