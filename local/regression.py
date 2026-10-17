#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psutil==7.2.2"]
# ///
"""Differential RowBinary tests and native or Compose ClickHouse benchmarks.

Run: uv run local/regression.py [--compose] [--profile quick|routine|stress]
Native mode starts isolated servers; --compose tests the running Compose service.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import csv
import hashlib
import json
import os
import platform
import random
import select
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from xml.sax.saxutils import escape

import psutil

ROOT = Path(__file__).resolve().parents[1]
REV = "e350b48f45bea062ac1af152d54698b1b7aee568"
IMPLEMENTATIONS = {"baseline": "count_tokens", "executor": "count_token_chrs"}
TIMEOUT = 120
PROFILES = {
    "quick": {
        "sizes": [(128, 256), (32, 4096), (8, 65536)],
        "repeats": 2,
        "workers": [1],
    },
    "routine": {
        "sizes": [(10000, 256), (1000, 4096), (128, 65536)],
        "repeats": 5,
        "workers": [1, 4],
    },
    "stress": {
        "sizes": [(50000, 256), (5000, 4096), (512, 65536)],
        "repeats": 10,
        "workers": [1, 2, 4],
    },
}


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def string(value):
    return varint(len(value)) + value


def rowbinary(rows):
    return b"".join(string(model) + string(text) for model, text in rows)


def corpus(seed):
    rng = random.Random(seed)
    rules = json.loads((ROOT / "udf/count_tokens_rs/models.json").read_text())
    models = [rule["prefix"].encode() + b"-test" for rule in rules]
    models += [
        b"",
        b"unknown-model",
        b"GPT-4",
        b"gpt-4o",
        b"gpt-4",
        b"gpt-4.1",
        b"gpt-4\xff",
    ]
    texts = [
        b"",
        b"hello world",
        b"\x00\n\r\t",
        b"\xff\xfe\xc0\xaf\xed\xa0\x80",
        "你好 café e\u0301 👩🏽‍💻 مرحبا".encode(),
        b"<|endoftext|>",
    ]
    rows = [(model, text) for model in models for text in texts]
    # LEB128 transitions and a row exceeding the 1 MiB input buffer. Avoid
    # pathological BPE behavior from megabytes of one repeated character.
    for size in [1, 127, 128, 255, 16383, 16384, (1 << 20) + 37]:
        rows.append((models[size % len(models)], payload(size, size)))
    for _ in range(128):
        model = rng.choice(models + [rng.randbytes(rng.randrange(32))])
        text = (
            rng.randbytes(rng.randrange(512))
            if rng.randrange(2)
            else payload(rng.randrange(4096), rng.randrange(10000))
        )
        rows.append((model, text))
    return rows


def payload(size, seed):
    rng = random.Random(seed)
    words = [
        "hello",
        "token",
        "query",
        "model",
        "ClickHouse",
        "123",
        "café",
        "你好",
        "🙂",
        "line\n",
        "{}",
        "\x00",
    ]
    # A randomized phrase, repeated with whitespace, is bounded and reproducible.
    phrase = (" ".join(rng.choices(words, k=128)) + " ").encode()
    return (phrase * (size // len(phrase) + 1))[:size]


class Worker:
    """Interact with a pool worker without prefetching the next request.

    Multiplex reads and writes: a large request can fill stdout before its
    entire stdin has been written. communicate() cannot model pooled exchanges.
    """

    def __init__(self, binary, command_prefix=()):
        self.errors = tempfile.TemporaryFile()  # noqa: SIM115 - owned until close()
        self.process = subprocess.Popen(
            [*command_prefix, str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.errors,
            cwd="/",
            start_new_session=True,
        )
        os.set_blocking(self.process.stdin.fileno(), False)
        os.set_blocking(self.process.stdout.fileno(), False)

    def exchange(self, rows, fragment=None):
        request = str(len(rows)).encode() + b"\n" + rowbinary(rows)
        pending = memoryview(request)
        result = bytearray()
        expected = len(rows) * 4
        deadline = time.monotonic() + TIMEOUT
        while pending or len(result) < expected:
            check(time.monotonic() < deadline, "worker exchange timed out")
            readable, writable, _ = select.select(
                [self.process.stdout], [self.process.stdin] if pending else [], [], 0.1
            )
            if writable:
                try:
                    count = os.write(
                        self.process.stdin.fileno(), pending[: fragment or 65536]
                    )
                except BrokenPipeError as error:
                    raise RuntimeError(
                        f"worker closed stdin: {self.diagnostics()}"
                    ) from error
                pending = pending[count:]
            if readable:
                data = os.read(self.process.stdout.fileno(), 65536)
                check(data, f"unexpected worker EOF: {self.diagnostics()}")
                result.extend(data)
                check(len(result) <= expected, "worker returned extra output")
        return bytes(result)

    def diagnostics(self):
        self.errors.seek(0)
        return self.errors.read().decode(errors="replace")[-4000:]

    def finish(self):
        self.process.stdin.close()
        self.process.wait(timeout=TIMEOUT)
        check(self.process.returncode == 0, f"worker failed: {self.diagnostics()}")
        check(
            not os.read(self.process.stdout.fileno(), 65536),
            "extra output after final chunk",
        )

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        for pipe in [self.process.stdin, self.process.stdout, self.errors]:
            pipe.close()


def build_and_stage(directory, skip_build):
    binaries = {}
    for impl, crate in [
        ("baseline", "count_tokens_rs"),
        ("executor", "count_token_chrs"),
    ]:
        manifest = ROOT / "udf" / crate / "Cargo.toml"
        if not skip_build:
            run(
                [
                    "cargo",
                    "build",
                    "--locked",
                    "--release",
                    "--manifest-path",
                    str(manifest),
                ]
            )
        binary = ROOT / "udf" / crate / "target/release" / IMPLEMENTATIONS[impl]
        check(binary.is_file(), f"missing release binary: {binary}")
        dest = directory / IMPLEMENTATIONS[impl]
        shutil.copy2(binary, dest)
        binaries[impl] = dest
    shutil.copy2(ROOT / "udf/count_tokens_rs/models.json", directory / "models.json")
    return binaries


def wire_tests(binaries, rows, command_prefix=(), configuration_tester=None):
    outputs = {}
    for impl, binary in binaries.items():
        worker = Worker(binary, command_prefix)
        try:
            # Zero-row chunks have no output acknowledgement. A following
            # nonempty exchange proves alignment, and final EOF proves shutdown.
            worker.exchange([])
            chunks = [rows[:5], rows[5:100], rows[100:]]
            output = b"".join(
                worker.exchange(chunk, fragment=7 if i == 0 else None)
                for i, chunk in enumerate(chunks)
            )
            worker.exchange([])
            check(
                worker.exchange(rows[:5], fragment=1) == output[:20],
                "worker reuse changed output",
            )
            worker.finish()
            outputs[impl] = output
        finally:
            worker.close()
        # Clean EOF before any chunk must also succeed.
        result = subprocess.run(
            [*command_prefix, str(binary)],
            input=b"",
            capture_output=True,
            cwd="/",
            timeout=TIMEOUT,
            check=False,
        )
        check(result.returncode == 0 and not result.stdout, f"{impl}: clean EOF failed")
        for wire in [
            b"bad\n",
            b"-1\n",
            b"1\n",
            b"1\n\x03ab",
            b"1\n\x01m\x03ab",
            b"2\n\x00\x00",
        ]:
            result = subprocess.run(
                [*command_prefix, str(binary)],
                input=wire,
                capture_output=True,
                cwd="/",
                timeout=TIMEOUT,
                check=False,
            )
            check(
                result.returncode != 0,
                f"{impl}: malformed/truncated input accepted: {wire!r}",
            )
        if impl == "executor":
            for wire in [
                b" 0\n",
                b"+0\n",
                b"0 \n",
                b"0\r\n",
                b"0",
                b"18446744073709551616\n",
            ]:
                result = subprocess.run(
                    [*command_prefix, str(binary)],
                    input=wire,
                    capture_output=True,
                    cwd="/",
                    timeout=TIMEOUT,
                    check=False,
                )
                check(
                    result.returncode != 0,
                    f"Executor accepted a malformed chunk header: {wire!r}",
                )
        if configuration_tester is not None:
            configuration_tester(binary)
            continue
        with tempfile.TemporaryDirectory(prefix="udf-config-") as tmp:
            isolated = Path(tmp) / binary.name
            shutil.copy2(binary, isolated)
            for config in [None, b"{invalid JSON"]:
                if config is not None:
                    (Path(tmp) / "models.json").write_bytes(config)
                result = subprocess.run(
                    [str(isolated)],
                    input=b"",
                    capture_output=True,
                    cwd="/",
                    timeout=TIMEOUT,
                    check=False,
                )
                check(
                    result.returncode != 0 and b"models.json" in result.stderr,
                    f"{impl}: missing/invalid config did not fail with diagnostics",
                )
    check(outputs["baseline"] == outputs["executor"], "wire outputs differ")
    expected = list(struct.unpack(f"<{len(rows)}I", outputs["baseline"]))
    # Simple independent checks make sure a broken baseline cannot make every
    # differential assertion pass trivially.
    for (_, text), count in zip(rows, expected):
        if text == b"":
            check(count == 0, "empty text must return zero")
        if text == b"hello world":
            check(count == 2, "known token count differs")
    print(
        f"Wire regression: {len(rows)} rows identical, reuse/fragmentation/errors passed",
        flush=True,
    )
    return expected


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, clickhouse, scripts, implementations, workers, directory):
        self.directory = directory
        directory.mkdir()
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}/"
        self.process = None
        self.children = set()
        functions = []
        for impl in implementations:
            name = IMPLEMENTATIONS[impl]
            functions.append(f"""<function><type>executable_pool</type><name>{name}</name>
                <return_type>UInt32</return_type><argument><type>String</type><name>model</name></argument>
                <argument><type>String</type><name>text</name></argument><format>RowBinary</format>
                <send_chunk_header>true</send_chunk_header><pool_size>{workers}</pool_size>
                <command>{name}</command><execute_direct>true</execute_direct><deterministic>true</deterministic>
                <max_command_execution_time>120</max_command_execution_time>
                <command_read_timeout>120000</command_read_timeout><command_write_timeout>120000</command_write_timeout>
                </function>""")
        (directory / "functions.xml").write_text(
            "<functions>" + "".join(functions) + "</functions>"
        )
        path = escape(str(directory))
        config = f"""<clickhouse>
            <logger><level>warning</level><log>{path}/server.log</log><errorlog>{path}/error.log</errorlog></logger>
            <path>{path}/data/</path><tmp_path>{path}/tmp/</tmp_path>
            <user_files_path>{path}/files/</user_files_path>
            <listen_host>127.0.0.1</listen_host><http_port>{self.port}</http_port>
            <user_scripts_path>{escape(str(scripts))}/</user_scripts_path>
            <user_defined_executable_functions_config>{path}/functions.xml</user_defined_executable_functions_config>
            <max_thread_pool_size>256</max_thread_pool_size>
            <background_schedule_pool_size>4</background_schedule_pool_size>
            <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
            <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
            <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
            <background_pool_size>16</background_pool_size><background_move_pool_size>2</background_move_pool_size>
            <background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size>
            <mark_cache_size>33554432</mark_cache_size><uncompressed_cache_size>8388608</uncompressed_cache_size>
            <profiles><default><max_threads>4</max_threads><log_queries>1</log_queries><log_profile_events>1</log_profile_events></default></profiles>
            <users><default><password></password><networks><ip>127.0.0.1</ip></networks>
                <profile>default</profile><quota>default</quota></default></users>
            <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors>
                <result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
            <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
        </clickhouse>"""
        (directory / "config.xml").write_text(config)
        self.console = (directory / "console.log").open("wb")
        self.process = subprocess.Popen(
            [clickhouse, "server", f"--config-file={directory / 'config.xml'}"],
            stdout=self.console,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                check(
                    self.process.poll() is None,
                    f"ClickHouse exited; see {directory / 'console.log'}",
                )
                try:
                    if self.query("SELECT 1", timeout=1).strip() == b"1":
                        return
                except (OSError, RuntimeError):
                    time.sleep(0.1)
            raise RuntimeError(
                f"ClickHouse startup timed out; see {directory / 'console.log'}"
            )
        except BaseException:
            self.close()
            raise

    def query(self, sql, body=b"", query_id=None, timeout=TIMEOUT):
        params = {"query": sql, "wait_end_of_query": "1"}
        if query_id:
            params["query_id"] = query_id
        request = urllib.request.Request(
            self.url + "?" + urllib.parse.urlencode(params), data=body
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            raise RuntimeError(error.read().decode(errors="replace")) from error

    def json(self, sql):
        return [
            json.loads(line)
            for line in self.query(sql + " FORMAT JSONEachRow").splitlines()
        ]

    def close(self):
        if self.process is not None:
            with contextlib.suppress(psutil.Error):
                self.children.update(
                    psutil.Process(self.process.pid).children(recursive=True)
                )
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    # The server and its workers belong to our dedicated session.
                    os.killpg(self.process.pid, 9)
                    self.process.wait(timeout=10)
            for child in self.children:
                with contextlib.suppress(psutil.Error):
                    child.kill()
            self.console.close()


def clickhouse_correctness(server, rows, expected):
    server.query(
        "CREATE TABLE regression (id UInt64, model String, text String) ENGINE=Memory"
    )
    wire = b"".join(
        struct.pack("<Q", i) + rowbinary([row]) for i, row in enumerate(rows)
    )
    server.query("INSERT INTO regression FORMAT RowBinary", wire)
    actual = server.query(
        "SELECT id, count_tokens(model,text), count_token_chrs(model,text) FROM regression ORDER BY id FORMAT RowBinary"
    )
    check(
        len(actual) == len(rows) * 16, "ClickHouse regression result row count differs"
    )
    for i, (index, baseline, executor) in enumerate(struct.iter_unpack("<QII", actual)):
        check(
            index == i and baseline == executor == expected[i],
            f"ClickHouse row {i} differs",
        )
    # Four distinct arguments per source row ensure all four calls are evaluated.
    columns = [
        "text",
        "concat(text,' suffix')",
        "reverse(text)",
        "substring(text,1,17)",
    ]
    results = []
    for name in IMPLEMENTATIONS.values():
        expressions = ",".join(f"{name}(model,{column})" for column in columns)
        results.append(
            server.query(
                f"SELECT id,{expressions} FROM regression ORDER BY id FORMAT RowBinary"
            )
        )
    check(
        results[0] == results[1] and len(results[0]) == len(rows) * 24,
        "four-column regression differs",
    )
    print(
        f"ClickHouse regression: {len(rows)} rows and four-column outputs identical",
        flush=True,
    )


class Sampler:
    """Sample only descendants of our server with an exact staged binary path.

    Worker RSS excludes the server. CPU deltas are taken at query boundaries;
    short-lived workers not observed by the sampler are reported as a limitation.
    """

    def __init__(self, server, binary):
        self.server = server
        self.binary = str(binary)
        self.stop = threading.Event()
        self.peak_rss = None
        self.peak_processes = 0
        self.seen = {}
        self.start_cpu = {}
        self.server_peak_rss = None
        self.worker_exit = False
        self.failure = None
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def sample(self, initial=False):
        total_rss = 0
        observed = 0
        parent = psutil.Process(self.server.process.pid)
        self.server_peak_rss = max(self.server_peak_rss or 0, parent.memory_info().rss)
        for process in parent.children(recursive=True):
            try:
                if process.exe() != self.binary:
                    continue
                key = (process.pid, process.create_time())
                cpu = process.cpu_times()
                total_rss += process.memory_info().rss
                observed += 1
                if initial:
                    self.start_cpu[key] = cpu.user + cpu.system
                self.seen[key] = (process, cpu.user + cpu.system)
                self.server.children.add(process)
            except psutil.Error:
                continue
        if observed:
            self.peak_rss = max(self.peak_rss or 0, total_rss)
        self.peak_processes = max(self.peak_processes, observed)

    def loop(self):
        while not self.stop.wait(0.02):
            try:
                self.sample()
            except (psutil.Error, OSError) as error:
                self.failure = error
                return

    def __enter__(self):
        self.sample(initial=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=5)
        self.sample()
        check(self.failure is None, f"worker resource sampling failed: {self.failure}")

    def metrics(self, wall):
        cpu = 0
        for key, (process, last_cpu) in self.seen.items():
            try:
                alive = (
                    process.is_running() and process.status() != psutil.STATUS_ZOMBIE
                )
            except psutil.Error:
                alive = False
            if not alive:
                self.worker_exit = True
            cpu += max(0, last_cpu - self.start_cpu.get(key, 0))
        check(
            not self.worker_exit,
            "a sampled UDF worker exited unexpectedly during a successful query",
        )
        return {
            "worker_sampled_peak_rss_bytes": self.peak_rss,
            "worker_sampled_processes": self.peak_processes,
            "worker_sampled_cpu_s": cpu if self.seen else None,
            "worker_sampled_average_cores": cpu / wall if self.seen else None,
            "server_sampled_peak_rss_bytes": self.server_peak_rss,
        }


EVENTS = {
    "udf_invocations": ("ExecutableUserDefinedFunctionInvocations", 1),
    "udf_elapsed_s": ("ExecutableUserDefinedFunctionElapsedMicroseconds", 1e6),
    "udf_pool_wait_s": ("ExecutableUserDefinedFunctionPoolWaitMicroseconds", 1e6),
    "udf_user_cpu_s": ("ExecutableUserDefinedFunctionUserTimeMicroseconds", 1e6),
    "udf_system_cpu_s": ("ExecutableUserDefinedFunctionSystemTimeMicroseconds", 1e6),
    "udf_peak_memory_byte_seconds": (
        "ExecutableUserDefinedFunctionPeakMemoryByteSeconds",
        1,
    ),
    "udf_input_bytes": ("ExecutableUserDefinedFunctionInputBytes", 1),
    "udf_output_bytes": ("ExecutableUserDefinedFunctionOutputBytes", 1),
}


def measure(
    server,
    binary,
    sql,
    concurrency,
    expected_rows,
    calls,
    metadata,
    sampler_factory=Sampler,
):
    ids = ["token-regression-" + uuid.uuid4().hex for _ in range(concurrency)]

    def query(query_id):
        start = time.perf_counter()
        # Explicit query IDs tie every measurement to exactly one query_log row.
        result = [
            json.loads(line)
            for line in server.query(
                sql
                + f" SETTINGS use_query_cache=0, max_threads=1, max_block_size={metadata['block_size']} FORMAT JSONEachRow",
                query_id=query_id,
            ).splitlines()
        ]
        check(
            len(result) == 1 and int(result[0]["n"]) == expected_rows,
            "benchmark returned incorrect source row count",
        )
        return time.perf_counter() - start, int(result[0]["tokens"])

    with sampler_factory(server, binary) as sampler:
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            responses = list(pool.map(query, ids))
        wall = time.perf_counter() - start
    server.query("SYSTEM FLUSH LOGS")
    quoted_ids = ",".join("'" + query_id + "'" for query_id in ids)
    logs = server.json(
        f"SELECT query_id,query_duration_ms,read_rows,result_rows,memory_usage,ProfileEvents FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({quoted_ids})"
    )
    check(len(logs) == concurrency, "missing or duplicate query-log records")
    events = [log["ProfileEvents"] for log in logs]
    record = dict(
        metadata,
        concurrency=concurrency,
        client_wall_s=wall,
        client_query_wall_s=[response[0] for response in responses],
        checksums=[response[1] for response in responses],
        query_ids=ids,
        source_rows=expected_rows * concurrency,
        udf_input_rows=expected_rows * calls * concurrency,
        udf_output_rows=expected_rows * calls * concurrency,
        query_result_rows=sum(int(log["result_rows"]) for log in logs),
        query_read_rows=sum(int(log["read_rows"]) for log in logs),
        server_query_duration_ms=[int(log["query_duration_ms"]) for log in logs],
        server_query_duration_max_ms=max(int(log["query_duration_ms"]) for log in logs),
        server_query_memory_bytes=[int(log["memory_usage"]) for log in logs],
        source_rows_per_s=expected_rows * concurrency / wall,
        udf_rows_per_s=expected_rows * calls * concurrency / wall,
        **sampler.metrics(wall),
    )
    for key, (event, divisor) in EVENTS.items():
        # Maps omit zero counters too. If absent, we cannot distinguish a zero
        # from an unsupported/unattributed event; preserve null, never fake zero.
        record[key] = (
            sum(int(e[event]) for e in events) / divisor
            if all(event in e for e in events)
            else None
        )
    record["udf_cpu_s"] = (
        record["udf_user_cpu_s"] + record["udf_system_cpu_s"]
        if record["udf_user_cpu_s"] is not None
        and record["udf_system_cpu_s"] is not None
        else None
    )
    record["udf_cpu_average_cores"] = (
        record["udf_cpu_s"] / wall if record["udf_cpu_s"] is not None else None
    )
    record["raw_query_log"] = logs
    check(
        record["query_result_rows"] == concurrency,
        "benchmark result row accounting differs",
    )
    check(
        record["query_read_rows"] == record["source_rows"],
        "benchmark source row accounting differs",
    )
    if record["udf_output_bytes"] is not None:
        check(
            record["udf_output_bytes"] == record["udf_output_rows"] * 4,
            "UDF pipe output does not match expected UInt32 result count",
        )
    return record


def create_workloads(server, profile, seed, metadata):
    workloads = []
    for label, (count, size) in zip(
        ["short", "medium", "long"], PROFILES[profile]["sizes"]
    ):
        table = "bench_" + label
        server.query(
            f"CREATE TABLE {table} (model String,t0 String,t1 String,t2 String,t3 String) ENGINE=Memory"
        )
        parts = []
        for i in range(count):
            model = [b"gpt-4", b"gpt-4o", b"gpt-4.1", b"unknown"][i % 4]
            # Distinct columns prevent common-subexpression elimination.
            texts = [
                payload(size + (i % 17), seed + (i % 32) * 4 + j) for j in range(4)
            ]
            parts.append(string(model) + b"".join(map(string, texts)))
        wire = b"".join(parts)
        digest = hashlib.sha256(wire).hexdigest()
        hashes = metadata.setdefault("workload_input_sha256", {})
        check(
            hashes.setdefault(label, digest) == digest,
            "benchmark input changed between implementations",
        )
        server.query(f"INSERT INTO {table} FORMAT RowBinary", wire)
        workloads.append((label, count, size, table))
    return workloads


def save_results(output, metadata, records):
    (output / "results.json").write_text(
        json.dumps({"metadata": metadata, "measurements": records}, indent=2) + "\n"
    )
    if records:
        keys = [key for key in records[0] if key != "raw_query_log"]
        with (output / "results.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        key: json.dumps(value)
                        if isinstance(value, (list, dict))
                        else value
                        for key, value in record.items()
                        if key in keys
                    }
                )


def comparisons(records):
    groups = {}
    for record in records:
        if record["phase"] == "measured":
            key = (
                record["workload"],
                record["calls_per_row"],
                record["workers"],
                record["concurrency"],
                record["block_size"],
            )
            groups.setdefault(key, {}).setdefault(record["implementation"], []).append(
                record
            )
    metrics = [
        "client_wall_s",
        "server_query_duration_max_ms",
        "source_rows_per_s",
        "udf_rows_per_s",
        "udf_invocations",
        "udf_elapsed_s",
        "udf_pool_wait_s",
        "udf_cpu_s",
        "udf_cpu_average_cores",
        "worker_sampled_cpu_s",
        "worker_sampled_average_cores",
        "worker_sampled_peak_rss_bytes",
        "worker_sampled_processes",
        "server_sampled_peak_rss_bytes",
        "udf_peak_memory_byte_seconds",
        "udf_input_bytes",
        "udf_output_bytes",
        "source_rows",
        "udf_input_rows",
        "udf_output_rows",
        "query_read_rows",
        "query_result_rows",
    ]
    result = []
    for (
        workload,
        calls,
        workers,
        concurrency,
        block,
    ), implementations in groups.items():
        for metric in metrics:
            row = {
                "workload": workload,
                "calls_per_row": calls,
                "workers": workers,
                "concurrency": concurrency,
                "block_size": block,
                "metric": metric,
            }
            for impl in IMPLEMENTATIONS:
                values = [
                    record[metric]
                    for record in implementations.get(impl, [])
                    if record[metric] is not None
                ]
                row[impl + "_samples"] = len(values)
                for stat, fn in [
                    ("median", statistics.median),
                    ("min", min),
                    ("max", max),
                ]:
                    row[impl + "_" + stat] = fn(values) if values else None
            baseline, executor = row["baseline_median"], row["executor_median"]
            row["executor_over_baseline"] = (
                executor / baseline if baseline and executor is not None else None
            )
            result.append(row)
    return result


def save_comparisons(output, records):
    rows = comparisons(records)
    (output / "comparison.json").write_text(json.dumps(rows, indent=2) + "\n")
    if rows:
        with (output / "comparison.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def summary(records):
    print(
        "\nWarm measurements: client wall median [min, max]; Executor/baseline ratio",
        flush=True,
    )
    groups = {}
    for record in records:
        if record["phase"] == "measured":
            key = (
                record["workload"],
                record["calls_per_row"],
                record["workers"],
                record["concurrency"],
                record["block_size"],
            )
            groups.setdefault(key, {}).setdefault(record["implementation"], []).append(
                record
            )
    for key, implementations in groups.items():
        medians = {}
        parts = []
        for impl, samples in implementations.items():
            walls = [sample["client_wall_s"] for sample in samples]
            medians[impl] = statistics.median(walls)
            parts.append(
                f"{impl} {medians[impl]:.4f}s [{min(walls):.4f}, {max(walls):.4f}]"
            )
        ratio = medians["executor"] / medians["baseline"]
        workload, calls, workers, concurrency, block = key
        print(
            f"  {workload}, {calls} calls/row, pool {workers}, concurrency {concurrency}, block {block}: "
            f"{'; '.join(parts)}; {ratio:.3f}x"
        )
    print(
        "CPU, RSS, invocations, rows, and byte comparisons: comparison.csv; raw measurements: results.json/results.csv."
    )


def main():
    def terminate(_signum, _frame):
        raise SystemExit("Regression runner terminated")

    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, default="routine")
    parser.add_argument("--clickhouse", default=shutil.which("clickhouse"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "local/results" / time.strftime("%Y%m%d-%H%M%S"),
    )
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--correctness-only", action="store_true")
    parser.add_argument(
        "--compose",
        action="store_true",
        help="test the running Compose ClickHouse; no native Rust/ClickHouse required",
    )
    parser.add_argument(
        "--compose-file", type=Path, default=ROOT / "local/compose.yaml"
    )
    parser.add_argument(
        "--compose-project", help="Compose project name, if started with -p"
    )
    parser.add_argument(
        "--compose-url", help="override the discovered published HTTP URL"
    )
    args = parser.parse_args()
    if args.compose:
        from compose_regression import run_compose

        run_compose(args)
        return
    check(
        args.clickhouse,
        "clickhouse is not installed; pass --clickhouse /path/to/clickhouse",
    )
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    scripts = output / "scripts"
    scripts.mkdir()
    metadata = {
        "profile": args.profile,
        "seed": args.seed,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "physical_cpus": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "clickhouse": subprocess.check_output(
            [args.clickhouse, "--version"], text=True
        ).strip(),
        "clickhouse_rs_revision": REV,
        "baseline_lock_sha256": hashlib.sha256(
            (ROOT / "udf/count_tokens_rs/Cargo.lock").read_bytes()
        ).hexdigest(),
        "models_sha256": hashlib.sha256(
            (ROOT / "udf/count_tokens_rs/models.json").read_bytes()
        ).hexdigest(),
        "sampling_interval_s": 0.02,
        "notes": [
            "Each concurrent query uses max_threads=1; concurrency equals pool size.",
            "UDF input/output rows are derived from the verified workload; query read/result rows come from query_log.",
            "RSS is the sampled sum of worker resident memory, includes idle pool workers, and may double-count shared pages.",
            "20ms sampling can miss short peaks/processes; worker CPU deltas include minor sampling-boundary error.",
            "Null ProfileEvents mean absent/unattributed/unsupported; raw maps are retained.",
            "Peak-memory byte-seconds are integrated memory, not peak RSS.",
            "Only the first query on a fresh pool is cold; later warmup rows are excluded from summaries.",
        ],
    }
    records = []
    try:
        binaries = build_and_stage(scripts, args.skip_build)
        metadata["executor_lock_sha256"] = hashlib.sha256(
            (ROOT / "udf/count_token_chrs/Cargo.lock").read_bytes()
        ).hexdigest()
        metadata["binary_sha256"] = {
            impl: hashlib.sha256(binary.read_bytes()).hexdigest()
            for impl, binary in binaries.items()
        }
        rows = corpus(args.seed)
        metadata["corpus_sha256"] = hashlib.sha256(rowbinary(rows)).hexdigest()
        expected = wire_tests(binaries, rows)
        with contextlib.closing(
            Server(args.clickhouse, scripts, IMPLEMENTATIONS, 1, output / "correctness")
        ) as server:
            metadata["server_version"] = (
                server.query("SELECT version()").decode().strip()
            )
            clickhouse_correctness(server, rows, expected)
        metadata["correctness"] = {"passed": True, "corpus_rows": len(rows)}
        if args.correctness_only:
            return
        reference = {}
        for workers in PROFILES[args.profile]["workers"]:
            order = (
                ["baseline", "executor"] if workers == 1 else ["executor", "baseline"]
            )
            for impl in order:
                with contextlib.closing(
                    Server(
                        args.clickhouse,
                        scripts,
                        [impl],
                        workers,
                        output / f"{impl}-{workers}",
                    )
                ) as server:
                    workloads = create_workloads(
                        server, args.profile, args.seed, metadata
                    )
                    # First query initializes both tokenizers; record startup cost
                    # separately before any warmup or measured workload.
                    name = IMPLEMENTATIONS[impl]
                    cold_sql = f"SELECT count() AS n,sum({name}(model,text)) AS tokens FROM values('model String,text String',('gpt-4','hello world'),('gpt-4o','hello world'))"
                    cold = measure(
                        server,
                        binaries[impl],
                        cold_sql,
                        1,
                        2,
                        1,
                        {
                            "implementation": impl,
                            "workers": workers,
                            "block_size": 256,
                            "workload": "startup",
                            "text_size_bytes": 11,
                            "calls_per_row": 1,
                            "phase": "cold",
                            "repetition": 0,
                        },
                    )
                    check(cold["checksums"] == [4], "cold query checksum differs")
                    records.append(cold)
                    metadata.setdefault("observed_profile_events", {})[
                        f"{impl}-{workers}"
                    ] = server.json(
                        "SELECT event,description FROM system.events WHERE event LIKE 'ExecutableUserDefinedFunction%'"
                    )
                    for label, count, size, table in workloads:
                        for calls in [1, 4]:
                            sql = (
                                "SELECT count() AS n,sum("
                                + "+".join(f"{name}(model,t{j})" for j in range(calls))
                                + f") AS tokens FROM {table}"
                            )
                            blocks = (
                                [256, 8192]
                                if args.profile == "stress"
                                else [256 if workers == 1 else 8192]
                            )
                            for block in blocks:
                                for repetition in range(
                                    PROFILES[args.profile]["repeats"] + 1
                                ):
                                    record = measure(
                                        server,
                                        binaries[impl],
                                        sql,
                                        workers,
                                        count,
                                        calls,
                                        {
                                            "implementation": impl,
                                            "workers": workers,
                                            "block_size": block,
                                            "workload": label,
                                            "text_size_bytes": size,
                                            "calls_per_row": calls,
                                            "phase": "warmup"
                                            if repetition == 0
                                            else "measured",
                                            "repetition": repetition,
                                        },
                                    )
                                    key = (label, calls)
                                    checksum = record["checksums"][0]
                                    check(
                                        all(
                                            value == checksum
                                            for value in record["checksums"]
                                        ),
                                        "concurrent checksums differ",
                                    )
                                    check(
                                        checksum == reference.setdefault(key, checksum),
                                        f"{impl}: benchmark checksum differs",
                                    )
                                    records.append(record)
                                    save_results(output, metadata, records)
                                print(
                                    f"{impl}: {label}, {calls} calls/row, {workers} workers, block {block} completed",
                                    flush=True,
                                )
        summary(records)
    except BaseException as error:
        metadata["failure"] = str(error)
        raise
    finally:
        save_results(output, metadata, records)
        save_comparisons(output, records)
        print(f"Results and server logs: {output}", flush=True)


if __name__ == "__main__":
    main()
