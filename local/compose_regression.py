"""Compose backend for regression.py; run `uv run local/regression.py --compose`."""

from __future__ import annotations

import base64
import hashlib
import json
import platform
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

import regression as core

# Read only the server and tokenizer processes. Runs outside the timed query,
# requiring neither Python nor psutil inside the ClickHouse image.
SNAPSHOT = r"""
for entry in /proc/[0-9]*; do
    exe=$(readlink "$entry/exe" 2>/dev/null) || continue
    case "$exe" in
        /opt/token-udfs/count_tokens_main|/opt/token-udfs/count_token_chrs|/usr/bin/clickhouse)
            stat=$(cat "$entry/stat" 2>/dev/null) || continue
            printf '%s|%s\n' "$exe" "$stat"
            ;;
    esac
done
"""


class ComposeServer:
    def __init__(self, args):
        compose = ["docker", "compose", "-f", str(args.compose_file.resolve())]
        if args.compose_project:
            compose += ["-p", args.compose_project]
        ids = subprocess.check_output(
            compose + ["ps", "-q", "clickhouse"], text=True
        ).split()
        core.check(
            len(ids) == 1,
            "Start Compose first: docker compose -f local/compose.yaml up -d --wait",
        )
        self.container = ids[0]
        info = json.loads(
            subprocess.check_output(["docker", "inspect", self.container])
        )[0]
        core.check(info["State"]["Running"], "Compose ClickHouse is not running")
        env = dict(item.split("=", 1) for item in info["Config"]["Env"] if "=" in item)
        credentials = f"{env.get('CLICKHOUSE_USER', 'benchmark')}:{env.get('CLICKHOUSE_PASSWORD', 'benchmark')}"
        self.auth = "Basic " + base64.b64encode(credentials.encode()).decode()
        ports = info["NetworkSettings"]["Ports"].get("8123/tcp") or []
        core.check(
            args.compose_url or ports,
            "Publish HTTP port 8123 or pass --compose-url http://host:port",
        )
        self.url = args.compose_url or f"http://127.0.0.1:{ports[0]['HostPort']}"
        self.database = None
        self.marker = uuid.uuid4().hex
        self.command_prefix = [
            "docker",
            "exec",
            "-i",
            "-w",
            "/",
            "-e",
            f"TOKEN_REGRESSION_RUN_ID={self.marker}",
            self.container,
        ]
        self.binaries = {
            "baseline": Path("/opt/token-udfs/count_tokens_main"),
            "executor": Path("/opt/token-udfs/count_token_chrs"),
        }
        self.pool_sizes = {}
        functions = ET.fromstring(
            self.exec("cat", "/etc/clickhouse-server/token-udfs.xml")
        )
        for impl, name in core.IMPLEMENTATIONS.items():
            matches = [
                fn
                for fn in functions.findall("function")
                if fn.findtext("name") == name
            ]
            core.check(
                len(matches) == 1, f"Missing Compose function definition: {name}"
            )
            fn = matches[0]
            core.check(
                fn.findtext("format") == "RowBinary"
                and fn.findtext("send_chunk_header") == "true",
                f"{name} must use RowBinary and chunk headers",
            )
            self.pool_sizes[impl] = int(fn.findtext("pool_size", "1"))
        core.check(
            len(set(self.pool_sizes.values())) == 1
            and min(self.pool_sizes.values()) > 0,
            "Configure equal, positive UDF pool sizes for a fair comparison",
        )
        status = self.exec("cat", "/proc/1/status").decode().splitlines()
        self.server_uid = next(
            line.split()[1] for line in status if line.startswith("Uid:")
        )
        units = self.exec("sh", "-c", "getconf CLK_TCK; getconf PAGESIZE").split()
        self.ticks, self.page_size = map(int, units)
        self.info = {
            "container_id": self.container,
            "image_id": info["Image"],
            "image": info["Config"]["Image"],
            "cpuset": info["HostConfig"]["CpusetCpus"],
            "nano_cpus": info["HostConfig"]["NanoCpus"],
            "pool_sizes": self.pool_sizes,
            "architecture": self.exec("uname", "-m").decode().strip(),
            "effective_cpus": next(
                line.split()[1]
                for line in status
                if line.startswith("Cpus_allowed_list:")
            ),
            "clock_ticks_per_second": self.ticks,
            "page_size_bytes": self.page_size,
        }

    def exec(self, *command, input=None, user=None):
        prefix = ["docker", "exec", "-i"]
        if user is not None:
            prefix += ["--user", str(user)]
        return subprocess.check_output(
            [*prefix, self.container, *command],
            input=input,
            timeout=core.TIMEOUT,
        )

    def query(self, sql, body=b"", query_id=None, timeout=core.TIMEOUT):
        params = {
            "query": sql,
            "wait_end_of_query": "1",
            "log_queries": "1",
            "log_profile_events": "1",
        }
        if self.database:
            params["database"] = self.database
        if query_id:
            params["query_id"] = query_id
        request = urllib.request.Request(
            self.url.rstrip("/") + "/?" + urllib.parse.urlencode(params),
            data=body,
            headers={"Authorization": self.auth},
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

    def snapshot(self):
        result = {}
        for line in (
            self.exec("sh", "-c", SNAPSHOT, user=self.server_uid).decode().splitlines()
        ):
            exe, stat = line.split("|", 1)
            pid = int(stat.split(" ", 1)[0])
            fields = stat.rsplit(")", 1)[
                1
            ].split()  # field 3 onward; comm can contain spaces
            result[(pid, int(fields[19]))] = {
                "exe": exe,
                "state": fields[0],
                "cpu": (int(fields[11]) + int(fields[12])) / self.ticks,
                "rss": int(fields[21]) * self.page_size,
            }
        core.check(
            any(pid == 1 for pid, _ in result),
            "Cannot read the container's ClickHouse process metrics",
        )
        return result

    def configuration_test(self, binary):
        directory = "/tmp/token-udf-config-" + uuid.uuid4().hex
        isolated = directory + "/" + binary.name
        self.exec("mkdir", "-m", "700", directory)
        try:
            self.exec("cp", str(binary), isolated)
            for config in [None, b"{invalid JSON"]:
                if config is not None:
                    self.exec("tee", directory + "/models.json", input=config)
                result = subprocess.run(
                    [*self.command_prefix, isolated],
                    input=b"",
                    capture_output=True,
                    timeout=core.TIMEOUT,
                    check=False,
                )
                core.check(
                    result.returncode != 0 and b"models.json" in result.stderr,
                    f"{binary.name}: missing/invalid configuration did not fail",
                )
        finally:
            self.exec("rm", "-f", isolated, directory + "/models.json")
            self.exec("rmdir", directory)

    def close(self):
        # Clean up only standalone wire-test workers carrying our random marker.
        # Pooled workers and the Compose server are left running.
        script = r"""
for entry in /proc/[0-9]*; do
    exe=$(readlink "$entry/exe" 2>/dev/null) || continue
    case "$exe" in
        /opt/token-udfs/count_tokens_main|/opt/token-udfs/count_token_chrs|/tmp/token-udf-config-*)
            if grep -zqx "TOKEN_REGRESSION_RUN_ID=$1" "$entry/environ" 2>/dev/null; then
                kill -KILL "${entry##*/}" 2>/dev/null || true
            fi
            ;;
    esac
done
"""
        self.exec("sh", "-c", script, "sh", self.marker)
        if self.database:
            database, self.database = self.database, None
            self.query(f"DROP DATABASE {database} SYNC")


class BoundarySampler:
    """Read container /proc before/after a timed query; no monitor runs during it.

    CPU deltas cover the query; RSS is observed at the boundaries and may miss
    transient peaks. Linux clock ticks are less precise than native psutil CPU.
    """

    def __init__(self, server, binary):
        self.server, self.binary = server, str(binary)

    def __enter__(self):
        self.before = self.server.snapshot()
        return self

    def __exit__(self, *args):
        self.after = self.server.snapshot()

    def metrics(self, wall):
        before = {
            key: value
            for key, value in self.before.items()
            if value["exe"] == self.binary
        }
        after = {
            key: value
            for key, value in self.after.items()
            if value["exe"] == self.binary
        }
        core.check(
            before.keys() <= after.keys()
            and all(v["state"] != "Z" for v in after.values()),
            "A UDF worker exited during the benchmark",
        )
        core.check(after, "No UDF worker was observed after the benchmark query")
        cpu = sum(
            value["cpu"] - before.get(key, {}).get("cpu", 0)
            for key, value in after.items()
        )
        rss = max(
            sum(v["rss"] for v in before.values()),
            sum(v["rss"] for v in after.values()),
        )
        server_rss = max(
            value["rss"]
            for snapshot in [self.before, self.after]
            for (pid, _), value in snapshot.items()
            if pid == 1
        )
        return {
            "worker_sampled_peak_rss_bytes": rss,
            "worker_sampled_processes": len(after),
            "worker_sampled_cpu_s": cpu,
            "worker_sampled_average_cores": cpu / wall,
            "server_sampled_peak_rss_bytes": server_rss,
        }


def run_compose(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "backend": "compose",
        "profile": args.profile,
        "seed": args.seed,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "clickhouse_rs_revision": core.REV,
        "sampling_interval_s": None,
        "notes": [
            "Attaches to the running Compose service; no pool resizing or cold-start measurements.",
            "workers is the configured pool size; concurrency is the number of concurrent queries.",
            "Each query uses max_threads=1. Implementations alternate measurement order.",
            "Worker CPU and RSS are read from container /proc before and after timed queries.",
            "RSS maxima cover these two observations only and can miss transient peaks.",
            "Idle workers of the measured implementation are included; shared RSS pages may be double-counted.",
            "UDF input/output rows are derived; read/result rows and pipe bytes come from query_log.",
            "Missing ProfileEvents remain null; peak-memory byte-seconds are not peak RSS.",
        ],
    }
    records = []
    server = None
    try:
        server = ComposeServer(args)
        metadata["docker"] = server.info
        metadata["server_version"] = server.query("SELECT version()").decode().strip()
        print(
            f"Attached to Compose ClickHouse {metadata['server_version']}; cpuset={server.info['cpuset'] or 'unrestricted'}",
            flush=True,
        )
        hashes = (
            server.exec(
                "sha256sum",
                *map(str, server.binaries.values()),
                "/opt/token-udfs/models.json",
            )
            .decode()
            .splitlines()
        )
        metadata["binary_sha256"] = {
            impl: hashes[i].split()[0] for i, impl in enumerate(server.binaries)
        }
        metadata["models_sha256"] = hashes[2].split()[0]
        core.check(
            metadata["models_sha256"]
            == hashlib.sha256(
                (core.ROOT / "udf/count_tokens_rs/models.json").read_bytes()
            ).hexdigest(),
            "Mounted models.json differs from this checkout; rebuild or update the mounted configuration",
        )
        rows = core.corpus(args.seed)
        metadata["corpus_sha256"] = hashlib.sha256(core.rowbinary(rows)).hexdigest()
        expected = core.wire_tests(
            server.binaries, rows, server.command_prefix, server.configuration_test
        )
        database = "token_regression_" + uuid.uuid4().hex
        server.query(f"CREATE DATABASE {database}")
        server.database = database
        core.clickhouse_correctness(server, rows, expected)
        metadata["correctness"] = {"passed": True, "corpus_rows": len(rows)}
        if args.correctness_only:
            return
        workloads = core.create_workloads(server, args.profile, args.seed, metadata)
        reference = {}
        for concurrency in core.PROFILES[args.profile]["workers"]:
            core.check(
                concurrency <= min(server.pool_sizes.values()),
                "Requested concurrency exceeds the configured UDF pool size",
            )
            blocks = (
                [256, 8192]
                if args.profile == "stress"
                else [256 if concurrency == 1 else 8192]
            )
            for label, count, size, table in workloads:
                for calls in [1, 4]:
                    for block in blocks:
                        for repetition in range(
                            core.PROFILES[args.profile]["repeats"] + 1
                        ):
                            order = (
                                ["baseline", "executor"]
                                if repetition % 2 == 0
                                else ["executor", "baseline"]
                            )
                            for impl in order:
                                name = core.IMPLEMENTATIONS[impl]
                                sql = (
                                    "SELECT count() AS n,sum("
                                    + "+".join(
                                        f"{name}(model,t{j})" for j in range(calls)
                                    )
                                    + f") AS tokens FROM {table}"
                                )
                                record = core.measure(
                                    server,
                                    server.binaries[impl],
                                    sql,
                                    concurrency,
                                    count,
                                    calls,
                                    {
                                        "implementation": impl,
                                        "workers": server.pool_sizes[impl],
                                        "block_size": block,
                                        "workload": label,
                                        "text_size_bytes": size,
                                        "calls_per_row": calls,
                                        "phase": "warmup"
                                        if repetition == 0
                                        else "measured",
                                        "repetition": repetition,
                                    },
                                    sampler_factory=BoundarySampler,
                                )
                                checksum = record["checksums"][0]
                                core.check(
                                    all(
                                        value == checksum
                                        for value in record["checksums"]
                                    ),
                                    "Concurrent checksums differ",
                                )
                                core.check(
                                    reference.setdefault((label, calls), checksum)
                                    == checksum,
                                    "Benchmark implementations differ",
                                )
                                records.append(record)
                                core.save_results(output, metadata, records)
                        print(
                            f"Compose: {label}, {calls} calls/row, concurrency {concurrency}, block {block} completed",
                            flush=True,
                        )
        metadata["observed_profile_events"] = server.json(
            "SELECT event,description FROM system.events WHERE event LIKE 'ExecutableUserDefinedFunction%'"
        )
        core.summary(records)
    except BaseException as error:
        metadata["failure"] = str(error)
        raise
    finally:
        try:
            if server is not None:
                server.close()
        finally:
            core.save_results(output, metadata, records)
            core.save_comparisons(output, records)
            print(f"Results: {output}", flush=True)
