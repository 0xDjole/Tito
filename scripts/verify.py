#!/usr/bin/env python3

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid


RUNTIME_VERSION = "7.1.6"
FILE_LIMIT = 16384
TIKV_CONFIG = """[storage]
reserve-space = "0MiB"
reserve-raft-space = "0MiB"

[storage.block-cache]
capacity = "512MiB"

[raftdb]
max-open-files = 1024

[raft-engine]
purge-threshold = "256MB"
target-file-size = "32MB"
"""
TITO_NATIVE_CASES = {
    "native_reads_forty_one_mib_records_through_byte_bounded_requests",
    "native_index_entries_of_a_large_record_hold_full_copies_keys_only_and_parts",
    "native_a_save_under_the_bound_commits_its_copies_and_a_larger_one_is_refused_before_writing",
    "native_queue_pulls_and_acks_sixteen_one_mib_events",
    "native_a_queue_handler_acks_its_own_pending_row_inside_its_transaction",
    "native_a_duplicate_delivery_conflicts_on_the_pending_row_and_its_retry_skips",
    "native_a_rolled_back_handler_transaction_leaves_its_row_pending_for_redelivery",
    "native_ack_in_tx_refuses_an_undelivered_event_a_row_of_another_queue_and_a_row_of_another_event",
    "native_every_read_for_share_or_for_update_in_a_writing_transaction_conflicts_with_an_update_of_its_record",
    "native_index_reads_scans_and_misses_add_no_guard",
    "native_two_transactions_reading_the_same_twenty_records_conflict_about_one_time_in_sixteen",
    "native_two_reads_for_update_of_one_record_always_conflict_and_the_retry_passes",
    "native_a_read_for_share_and_a_read_for_update_of_one_record_commit_together_in_either_order",
    "native_writers_that_read_one_record_unguarded_never_conflict",
    "native_an_unguarded_read_conflicts_with_no_update_or_remove_of_its_record",
    "native_an_unguarded_read_cancels_no_guard_of_its_transaction",
    "native_an_update_and_a_remove_conflict_with_reads_for_share_and_for_update_in_either_order",
    "native_a_transaction_that_writes_nothing_adds_no_guard_and_a_read_for_update_counts_as_a_write",
    "native_a_retry_starts_without_the_guards_of_the_attempt_before",
    "native_a_new_holder_and_a_delete_of_the_record_it_names_conflict_in_either_order",
    "native_concurrent_claims_of_one_unique_value_conflict_and_the_retry_is_refused",
    "native_unique_values_differing_only_by_case_or_spacing_coexist_and_are_found_exactly",
    "native_a_note_whose_key_holds_a_live_lock_waits_for_it_like_a_single_read",
    "native_a_key_a_batch_read_did_not_find_is_answered_from_memory_while_a_lock_sits_on_it",
    "native_a_batch_read_keeps_the_writes_and_locks_of_its_own_transaction",
    "native_a_create_sends_no_read_while_a_lock_sits_on_its_row",
    "native_an_update_sends_no_read_while_a_lock_sits_on_its_row",
}


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".pending")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def require_cases(output, cases):
    passed = set(re.findall(r"^test (\S+) \.\.\. ok$", output, re.MULTILINE))
    if not cases <= passed:
        raise RuntimeError(f"Mandatory cases did not pass: {sorted(cases - passed)}")
    results = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;",
        output,
        re.MULTILINE,
    )
    if not results or any(int(failed) or int(ignored) for _, failed, ignored in results):
        raise RuntimeError("Verification requires every selected case with zero failures and zero ignored")


def runtime_binary(path, name):
    if not path:
        raise RuntimeError(
            f"Tito verification needs the {name} {RUNTIME_VERSION} binary: pass --{name} or set "
            f"TITO_VERIFY_{name.upper()}"
        )
    binary = Path(path)
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"The {name} binary is missing or not executable: {binary}")
    version = subprocess.run(
        [str(binary), "--version"], capture_output=True, text=True, timeout=30, env={}
    )
    if version.returncode or RUNTIME_VERSION not in version.stdout:
        raise RuntimeError(f"The {name} binary {binary} is not version {RUNTIME_VERSION}")
    return binary


def raise_file_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = FILE_LIMIT if hard == resource.RLIM_INFINITY else min(hard, FILE_LIMIT)
    if soft != resource.RLIM_INFINITY and soft < wanted:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))


def source_hashes(root):
    paths = {
        root / "Cargo.toml",
        root / "Cargo.lock",
        root / "scripts/verify.py",
    }
    for directory in ["src", "tests", "examples"]:
        paths.update(
            path
            for path in (root / directory).rglob("*")
            if path.is_file()
            and path.suffix in {".rs", ".toml"}
            and "target" not in path.relative_to(root).parts
        )
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def cargo_target(root):
    declared = os.environ.get("CARGO_TARGET_DIR")
    path = Path(declared) if declared else root / "target"
    if not path.is_absolute():
        raise RuntimeError("An explicitly selected Cargo cache must be an absolute path")
    return path


class OwnedRuntime:
    def __init__(self, artifacts, run_id, binaries):
        self.artifacts = artifacts
        self.run_id = run_id
        self.binaries = binaries
        self.processes = []
        self.data = None
        self.endpoint = None
        self.cleanup_attempts = []
        self.artifacts.mkdir(mode=0o700)
        self.persist()

    def persist(self, **extra):
        write_json(
            self.artifacts / "runtime.json",
            {
                "run_id": self.run_id,
                "binaries": {name: str(path) for name, path in self.binaries.items()},
                "data": str(self.data) if self.data else None,
                "processes": [
                    {key: value for key, value in entry.items() if key != "process"}
                    for entry in self.processes
                ],
                "endpoint": self.endpoint,
                "cleanup_attempts": self.cleanup_attempts,
                **extra,
            },
        )

    def start_process(self, name, binary, arguments):
        log = self.artifacts / f"{name}.log"
        entry = {"name": name, "pid": None, "arguments": [str(binary), *arguments], "log": str(log)}
        self.processes.append(entry)
        with log.open("wb") as output:
            entry["process"] = subprocess.Popen(
                [str(binary), *arguments],
                cwd=self.data,
                env={},
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        entry["pid"] = entry["process"].pid
        self.persist()

    def start(self):
        self.data = Path(tempfile.mkdtemp(prefix=f"{self.run_id}-"))
        self.persist()
        with contextlib.ExitStack() as reservations:
            ports = []
            for _ in range(4):
                listener = reservations.enter_context(socket.socket())
                listener.bind(("127.0.0.1", 0))
                ports.append(listener.getsockname()[1])
        client, peer, store, status = ports
        self.endpoint = f"http://127.0.0.1:{client}"
        name = f"{self.run_id}-pd"
        self.persist()
        self.start_process(
            name,
            self.binaries["pd"],
            [
                f"--name={name}",
                f"--data-dir={self.data / name}",
                f"--client-urls={self.endpoint}",
                f"--advertise-client-urls={self.endpoint}",
                f"--peer-urls=http://127.0.0.1:{peer}",
                f"--advertise-peer-urls=http://127.0.0.1:{peer}",
                f"--initial-cluster={name}=http://127.0.0.1:{peer}",
            ],
        )
        self.wait_member(name)
        config = self.data / "tikv.toml"
        config.write_text(TIKV_CONFIG)
        self.start_process(
            f"{self.run_id}-tikv",
            self.binaries["tikv"],
            [
                f"--config={config}",
                f"--pd={self.endpoint}",
                f"--addr=127.0.0.1:{store}",
                f"--advertise-addr=127.0.0.1:{store}",
                f"--status-addr=127.0.0.1:{status}",
                f"--advertise-status-addr=127.0.0.1:{status}",
                f"--data-dir={self.data / 'tikv'}",
            ],
        )
        self.wait_store(f"127.0.0.1:{store}")

    def exited(self):
        return [
            entry["name"]
            for entry in self.processes
            if "process" in entry and entry["process"].poll() is not None
        ]

    def wait(self, path, predicate):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            stopped = self.exited()
            if stopped:
                raise RuntimeError(f"Owned {self.run_id} process exited before it was ready: {stopped}")
            try:
                with opener.open(self.endpoint + path, timeout=2) as response:
                    data = json.loads(response.read(65537))
                if predicate(data):
                    return
            except (OSError, ValueError, KeyError, TypeError):
                pass
            time.sleep(0.2)
        raise RuntimeError(f"Owned {self.run_id} did not become ready for {path}")

    def wait_member(self, name):
        cluster_id = None

        def ready(data):
            nonlocal cluster_id
            if cluster_id is None and data["header"]["cluster_id"] > 0:
                cluster_id = data["header"]["cluster_id"]
            return (
                data["header"]["cluster_id"] == cluster_id
                and [member["name"] for member in data["members"]] == [name]
                and data["leader"]["member_id"] == data["etcd_leader"]["member_id"]
                and [url for member in data["members"] for url in member["client_urls"]]
                == [self.endpoint]
            )

        self.wait("/pd/api/v1/members", ready)

    def wait_store(self, address):
        self.wait(
            "/pd/api/v1/stores",
            lambda data: any(
                entry["store"]["address"] == address
                and entry["store"]["state_name"] == "Up"
                for entry in data["stores"]
            ),
        )

    def close(self):
        failures = []
        for entry in reversed(self.processes):
            process = entry.get("process")
            if process is None:
                continue
            try:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=30)
                entry["exit_code"] = process.returncode
                log = Path(entry["log"])
                if log.exists():
                    entry["log_sha256"] = hashlib.sha256(log.read_bytes()).hexdigest()
            except Exception as error:
                failures.append(f"Process stop failed for {entry['name']}: {error}")
        running = [
            entry["name"]
            for entry in self.processes
            if "process" in entry and entry["process"].poll() is None
        ]
        if self.data is not None:
            try:
                shutil.rmtree(self.data)
            except FileNotFoundError:
                pass
            except Exception as error:
                failures.append(f"Data folder removal failed for {self.data}: {error}")
        remaining_data = self.data is not None and self.data.exists()
        cleanup = {
            "verified": not failures and not running and not remaining_data,
            "failures": failures,
            "running_processes": running,
            "remaining_data": remaining_data,
        }
        self.cleanup_attempts.append(cleanup)
        self.persist(cleanup=cleanup)
        if failures or running or remaining_data:
            raise RuntimeError(f"Owned runtime cleanup failed; inspect {self.artifacts / 'runtime.json'}")


class Verification:
    def __init__(self, root, binaries):
        self.root = root
        self.binaries = binaries
        self.run_id = "run-tito-" + uuid.uuid4().hex[:16]
        self.artifacts = root / "target/native-test-runs" / self.run_id
        self.artifacts.mkdir(parents=True, mode=0o700)
        self.inputs = source_hashes(root)
        write_json(self.artifacts / "inputs.json", self.inputs)
        self.environment = os.environ.copy()
        for key in list(self.environment):
            if key.startswith(("CARGO_MANIFEST_", "CARGO_PKG_")) or key in {
                "TITO_NATIVE_PD_URI", "TITO_NATIVE_RUN_ID"
            }:
                self.environment.pop(key)
        self.environment["CARGO_BUILD_JOBS"] = "4"
        self.environment["RUST_TEST_THREADS"] = "4"
        self.environment["CARGO_TERM_COLOR"] = "never"
        self.environment["CARGO_TARGET_DIR"] = str(cargo_target(root))
        self.phases = []
        self.current = None
        self.runtime = None

    def assert_inputs(self):
        if source_hashes(self.root) != self.inputs:
            raise RuntimeError("Verification inputs changed during the command")

    def command(self, phase, arguments, environment=None, timeout=None):
        self.assert_inputs()
        path = self.artifacts / f"{phase}.log"
        entry = {"phase": phase, "arguments": arguments, "started_at": time.time(), "log": str(path)}
        self.phases.append(entry)
        write_json(self.artifacts / "phases.json", self.phases)
        print(f"{phase}: {path}", flush=True)
        try:
            with path.open("wb") as output:
                self.current = subprocess.Popen(
                    arguments,
                    cwd=self.root,
                    env=environment or self.environment,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                code = self.current.wait(timeout=timeout)
        except BaseException:
            if self.current and self.current.poll() is None:
                os.killpg(self.current.pid, signal.SIGTERM)
                try:
                    self.current.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(self.current.pid, signal.SIGKILL)
                    self.current.wait()
            raise
        finally:
            self.current = None
            entry["finished_at"] = time.time()
            write_json(self.artifacts / "phases.json", self.phases)
        entry["exit_code"] = code
        write_json(self.artifacts / "phases.json", self.phases)
        if code:
            raise RuntimeError(f"{phase} failed with exit {code}; inspect {path}")
        self.assert_inputs()
        return path.read_text()

    def verify(self):
        self.command("tito-build", ["cargo", "test", "--locked", "--offline", "--all-targets", "--no-run"])
        self.runtime = OwnedRuntime(self.artifacts / "runtime", self.run_id, self.binaries)
        try:
            self.runtime.start()
            environment = {
                **self.environment,
                "TITO_NATIVE_PD_URI": self.runtime.endpoint.removeprefix("http://"),
                "TITO_NATIVE_RUN_ID": self.runtime.run_id,
            }
            output = self.command("tito-all-targets", ["cargo", "test", "--locked", "--offline", "--all-targets"], environment)
            require_cases(output, TITO_NATIVE_CASES)
        finally:
            self.runtime.close()
            self.runtime = None
        print(f"Complete Tito verification passed; evidence: {self.artifacts}", flush=True)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description="Complete Tito verification on owned PD and TiKV processes")
    parser.add_argument("--pd", default=os.environ.get("TITO_VERIFY_PD"))
    parser.add_argument("--tikv", default=os.environ.get("TITO_VERIFY_TIKV"))
    arguments = parser.parse_args()
    binaries = {
        "pd": runtime_binary(arguments.pd, "pd"),
        "tikv": runtime_binary(arguments.tikv, "tikv"),
    }
    os.umask(0o077)
    raise_file_limit()
    root = Path(__file__).resolve().parents[1]
    target = cargo_target(root)
    target.mkdir(parents=True, exist_ok=True)
    with (target / "native-verification.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another verification command owns this Tito checkout")
        signal.signal(signal.SIGINT, interrupted)
        signal.signal(signal.SIGTERM, interrupted)
        verification = Verification(root, binaries)
        try:
            verification.verify()
        finally:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            if verification.runtime is not None:
                verification.runtime.close()


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyboardInterrupt, subprocess.TimeoutExpired) as error:
        print(f"Tito verification refused: {error}", file=sys.stderr)
        sys.exit(1)
