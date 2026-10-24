#!/usr/bin/env python3

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid


PD_IMAGE = "pingcap/pd:v7.1.6@sha256:59c9896b8040bc183ef75f28e2d7972deeccf1c682336e3c9e9406e629c40dcc"
TIKV_IMAGE = "pingcap/tikv:v7.1.6@sha256:6b07644a3378d98a5069fb9036399640861a1b7a0458efec4a562fcc6212b890"
OWNER_LABEL = "org.arky.tito-native-run"
NATIVE_CASE = "pd::retry::native_recovery_tests::actual_pd_leadership_transfer_recovers_the_same_closed_oracle_client_with_monotonic_timestamps"
PROTOCOL_CASES = {
    "pd::retry::reconnect_regression_tests::a_closed_request_channel_waits_for_replacement_instead_of_exhausting_retries",
    "pd::retry::reconnect_regression_tests::a_stream_error_waits_for_replacement_instead_of_exhausting_retries",
    "pd::retry::reconnect_regression_tests::a_stream_eof_waits_for_replacement_instead_of_exhausting_retries",
    "pd::retry::reconnect_regression_tests::concurrent_failed_requests_share_one_replacement",
    "pd::retry::reconnect_regression_tests::a_throttled_wait_releases_the_lock_and_rechecks_a_concurrent_replacement",
    "pd::retry::reconnect_regression_tests::healthy_idleness_does_not_replace_the_connection",
    "pd::retry::reconnect_regression_tests::a_continued_stream_outage_keeps_the_existing_request_budget",
    "pd::retry::reconnect_regression_tests::a_continued_connection_outage_keeps_the_existing_connection_budget",
    "pd::retry::test::test_reconnect",
    "pd::retry::test::test_retry",
}
TITO_NATIVE_CASES = {
    "native_batch_swap_fences_every_source_and_commits_only_final_owners",
    "expired_secondary_without_a_primary_heals_through_normal_tito_reads",
    "native_reads_forty_one_mib_records_through_byte_bounded_requests",
    "native_queue_pulls_and_completes_sixteen_one_mib_events",
    "native_a_queue_handler_completes_its_own_pending_row_inside_its_transaction",
    "native_a_standalone_worker_handler_completes_its_own_pending_row",
    "native_a_duplicate_delivery_conflicts_on_the_pending_row_and_its_retry_skips",
    "native_a_rolled_back_handler_transaction_leaves_its_row_pending_for_redelivery",
    "native_ack_in_tx_refuses_an_undelivered_event_and_a_row_of_another_event",
    "native_a_new_pointer_to_a_one_stripe_target_conflicts_with_its_concurrent_removal",
    "native_a_new_pointer_to_a_one_stripe_target_conflicts_with_its_concurrent_deleting_mark",
    "native_new_pointers_to_a_one_stripe_target_from_sources_in_different_stripes_commit_together",
    "native_new_pointers_to_a_one_stripe_target_from_sources_in_one_stripe_conflict_and_the_retry_keeps_both",
    "native_one_stripe_shared_checks_conflict_with_each_other_and_with_an_update",
    "native_a_new_pointer_to_a_sixteen_stripe_target_conflicts_with_its_concurrent_removal_and_deleting_mark",
    "native_new_pointers_to_a_sixteen_stripe_target_from_sources_in_different_stripes_commit_together",
    "native_new_pointers_to_a_sixteen_stripe_target_from_sources_in_one_stripe_conflict_and_the_retry_keeps_both",
    "native_shared_checks_on_twenty_sixteen_stripe_records_from_transactions_in_different_stripes_commit_together",
    "native_shared_checks_on_a_sixteen_stripe_record_from_transactions_in_one_stripe_conflict_and_the_retry_passes",
    "native_a_sixteen_stripe_shared_check_conflicts_with_an_update_in_either_order",
    "native_assert_current_conflicts_with_a_concurrent_update_in_either_order",
    "native_assert_version_conflicts_with_a_concurrent_update_and_then_reports_the_change",
    "native_concurrent_claims_of_one_unique_value_conflict_and_the_retry_is_refused",
    "native_unique_values_differing_only_by_case_or_spacing_coexist_and_are_found_exactly",
}


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".pending")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def require_cases(output, cases, expected_count=None):
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
    if expected_count is not None and sum(int(count) for count, _, _ in results) != expected_count:
        raise RuntimeError("Executed case count differs from the exact mandatory discovery")


def docker(*arguments, check=True):
    result = subprocess.run(
        ["docker", *arguments], capture_output=True, text=True, timeout=60
    )
    if check and result.returncode:
        raise RuntimeError(f"Docker {arguments[0]} refused: {result.stderr.strip()}")
    return result


def source_hashes(root):
    paths = {
        root / "Cargo.toml",
        root / "Cargo.lock",
        root / "vendor/tikv-client/Cargo.toml",
        root / "vendor/tikv-client/Cargo.lock",
        root / "scripts/verify.py",
    }
    for directory in ["src", "tests", "examples", "vendor/tikv-client"]:
        paths.update(
            path
            for path in (root / directory).rglob("*")
            if path.is_file()
            and path.suffix in {".rs", ".proto", ".toml"}
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
    def __init__(self, artifacts, run_id, member_count, with_tikv):
        self.artifacts = artifacts
        self.run_id = run_id
        self.member_count = member_count
        self.with_tikv = with_tikv
        self.containers = []
        self.volumes = []
        self.endpoints = []
        self.cleanup_attempts = []
        self.artifacts.mkdir(mode=0o700)
        self.persist()

    def persist(self, **extra):
        write_json(
            self.artifacts / "runtime.json",
            {
                "run_id": self.run_id,
                "member_count": self.member_count,
                "containers": self.containers,
                "volumes": self.volumes,
                "endpoints": self.endpoints,
                "cleanup_attempts": self.cleanup_attempts,
                **extra,
            },
        )

    def create_volume(self, name):
        if docker("volume", "inspect", name, check=False).returncode == 0:
            raise RuntimeError(f"Refusing an existing volume: {name}")
        self.volumes.append(name)
        self.persist()
        docker("volume", "create", "--label", f"{OWNER_LABEL}={self.run_id}", name)

    def create_container(self, name, options, image, command):
        if docker("container", "inspect", name, check=False).returncode == 0:
            raise RuntimeError(f"Refusing an existing container: {name}")
        entry = {"name": name, "id": None}
        self.containers.append(entry)
        self.persist()
        entry["id"] = docker(
            "create",
            "--pull=never",
            "--name",
            name,
            "--label",
            f"{OWNER_LABEL}={self.run_id}",
            *options,
            image,
            *command,
        ).stdout.strip()
        self.persist()
        docker("start", entry["id"])
        return entry["id"]

    def start(self):
        images = [PD_IMAGE] + ([TIKV_IMAGE] if self.with_tikv else [])
        for image in images:
            docker("image", "inspect", "--format", "{{.Id}}", image)
            volumes = json.loads(
                docker("image", "inspect", "--format", "{{json .Config.Volumes}}", image).stdout
            ) or {}
            if set(volumes) - {"/data"}:
                raise RuntimeError("Pinned image declares an uncovered anonymous data volume")
        with contextlib.ExitStack() as reservations:
            ports = []
            for _ in range(self.member_count * 2 + (2 if self.with_tikv else 0)):
                listener = reservations.enter_context(socket.socket())
                listener.bind(("127.0.0.1", 0))
                ports.append(listener.getsockname()[1])
            clients = ports[: self.member_count]
            peers = ports[self.member_count : self.member_count * 2]
            self.endpoints = [f"http://127.0.0.1:{port}" for port in clients]
            members = [f"{self.run_id}-pd{index + 1}" for index in range(self.member_count)]
            initial_cluster = ",".join(
                f"{name}=http://127.0.0.1:{port}" for name, port in zip(members, peers)
            )
            for name in members:
                self.create_volume(f"{name}-data")
            if self.with_tikv:
                self.create_volume(f"{self.run_id}-tikv-data")
            self.persist()
        anchor = None
        for index, name in enumerate(members):
            options = [
                "--memory",
                "512m",
                "--cpus",
                "1",
                "--mount",
                f"type=volume,src={name}-data,dst=/data",
            ]
            if anchor is None:
                published = clients + (ports[-2:] if self.with_tikv else [])
                for port in published:
                    options.extend(["--publish", f"127.0.0.1:{port}:{port}"])
            else:
                options.extend(["--network", f"container:{anchor}"])
            container = self.create_container(
                name,
                options,
                PD_IMAGE,
                [
                    f"--name={name}",
                    "--data-dir=/data/pd",
                    f"--client-urls=http://0.0.0.0:{clients[index]}",
                    f"--advertise-client-urls={self.endpoints[index]}",
                    f"--peer-urls=http://0.0.0.0:{peers[index]}",
                    f"--advertise-peer-urls=http://127.0.0.1:{peers[index]}",
                    f"--initial-cluster={initial_cluster}",
                ],
            )
            if anchor is None:
                anchor = container
        self.wait_members()
        if self.with_tikv:
            config = self.artifacts / "tikv.toml"
            config.write_text(
                '[storage]\nreserve-space = "0MiB"\nreserve-raft-space = "0MiB"\n'
                '[storage.block-cache]\ncapacity = "512MiB"\n'
            )
            self.create_container(
                f"{self.run_id}-tikv",
                [
                    "--network",
                    f"container:{anchor}",
                    "--memory",
                    "3g",
                    "--cpus",
                    "2",
                    "--mount",
                    f"type=volume,src={self.run_id}-tikv-data,dst=/data",
                    "--mount",
                    f"type=bind,src={config},dst=/etc/tikv.toml,readonly",
                ],
                TIKV_IMAGE,
                [
                    "--config=/etc/tikv.toml",
                    f"--pd={','.join(self.endpoints)}",
                    f"--addr=0.0.0.0:{ports[-2]}",
                    f"--advertise-addr=127.0.0.1:{ports[-2]}",
                    f"--status-addr=0.0.0.0:{ports[-1]}",
                    f"--advertise-status-addr=127.0.0.1:{ports[-1]}",
                    "--data-dir=/data/tikv",
                ],
            )
            self.wait_store(f"127.0.0.1:{ports[-2]}")

    def wait(self, path, predicate, all_members=False):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                ready = True
                for endpoint in (self.endpoints if all_members else self.endpoints[:1]):
                    with opener.open(endpoint + path, timeout=2) as response:
                        data = json.loads(response.read(65537))
                    if not predicate(data):
                        ready = False
                        break
                if ready:
                    return
            except (OSError, ValueError, KeyError, TypeError):
                pass
            time.sleep(0.2)
        raise RuntimeError(f"Owned {self.run_id} did not become ready for {path}")

    def wait_members(self):
        expected = {f"{self.run_id}-pd{index + 1}" for index in range(self.member_count)}
        cluster_id = None

        def ready(data):
            nonlocal cluster_id
            if cluster_id is None and data["header"]["cluster_id"] > 0:
                cluster_id = data["header"]["cluster_id"]
            return (
                data["header"]["cluster_id"] == cluster_id
                and {member["name"] for member in data["members"]} == expected
                and len(data["members"]) == self.member_count
                and data["leader"]["member_id"] == data["etcd_leader"]["member_id"]
                and {url for member in data["members"] for url in member["client_urls"]}
                == set(self.endpoints)
            )

        self.wait("/pd/api/v1/members", ready, all_members=True)

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
        for entry in reversed(self.containers):
            name = entry["name"]
            reference = entry["id"] or name
            try:
                inspected = docker(
                    "container", "inspect", "--format", "{{json .Config.Labels}}", reference, check=False
                )
                if inspected.returncode:
                    continue
                labels = json.loads(inspected.stdout)
                if not isinstance(labels, dict) or labels.get(OWNER_LABEL) != self.run_id:
                    failures.append(f"Refused container with different ownership: {name}")
                    continue
                try:
                    logs = docker("logs", "--timestamps", reference, check=False)
                    for stream in ["stdout", "stderr"]:
                        path = self.artifacts / f"{name}.{stream}.log"
                        path.write_text(getattr(logs, stream))
                        entry[f"{stream}_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                    if logs.returncode:
                        failures.append(f"Native log capture refused: {name}")
                except Exception as error:
                    failures.append(f"Native log capture failed for {name}: {error}")
                if docker("rm", "--force", reference, check=False).returncode:
                    failures.append(f"Container removal refused: {name}")
            except Exception as error:
                failures.append(f"Container cleanup failed for {name}: {error}")
        for name in reversed(self.volumes):
            try:
                inspected = docker(
                    "volume", "inspect", "--format", "{{json .Labels}}", name, check=False
                )
                if inspected.returncode:
                    continue
                labels = json.loads(inspected.stdout)
                if not isinstance(labels, dict) or labels.get(OWNER_LABEL) != self.run_id:
                    failures.append(f"Refused volume with different ownership: {name}")
                    continue
                if docker("volume", "rm", name, check=False).returncode:
                    failures.append(f"Volume removal refused: {name}")
            except Exception as error:
                failures.append(f"Volume cleanup failed for {name}: {error}")
        remaining_containers = None
        remaining_volumes = None
        try:
            remaining_containers = docker(
                "ps", "--all", "--quiet", "--filter", f"label={OWNER_LABEL}={self.run_id}"
            ).stdout.split()
        except Exception as error:
            failures.append(f"Final container verification failed: {error}")
        try:
            remaining_volumes = docker(
                "volume", "ls", "--quiet", "--filter", f"label={OWNER_LABEL}={self.run_id}"
            ).stdout.split()
        except Exception as error:
            failures.append(f"Final volume verification failed: {error}")
        cleanup = {
            "verified": not failures and not remaining_containers and not remaining_volumes,
            "failures": failures,
            "remaining_containers": remaining_containers,
            "remaining_volumes": remaining_volumes,
        }
        self.cleanup_attempts.append(cleanup)
        self.persist(cleanup=cleanup)
        if failures or remaining_containers or remaining_volumes:
            raise RuntimeError(f"Owned runtime cleanup failed; inspect {self.artifacts / 'runtime.json'}")


class Verification:
    def __init__(self, root):
        self.root = root
        self.run_id = "run-tito-" + uuid.uuid4().hex[:16]
        self.artifacts = root / "target/native-test-runs" / self.run_id
        self.artifacts.mkdir(parents=True, mode=0o700)
        self.inputs = source_hashes(root)
        write_json(self.artifacts / "inputs.json", self.inputs)
        self.environment = os.environ.copy()
        for key in list(self.environment):
            if key.startswith(("CARGO_MANIFEST_", "CARGO_PKG_")) or key in {
                "PD_ADDRS", "TITO_NATIVE_PD_URI", "TITO_NATIVE_RUN_ID"
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
        vendor = [
            "cargo", "test", "--locked", "--offline", "--manifest-path", "vendor/tikv-client/Cargo.toml",
            "--no-default-features", "--features", "integration-tests", "--lib",
        ]
        self.command("tito-build", ["cargo", "test", "--locked", "--offline", "--all-targets", "--no-run"])
        self.command("vendor-build", vendor + ["--no-run"])
        listing = self.command("vendor-discovery", vendor + ["pd::retry::", "--", "--list"])
        discovered = {line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")}
        if discovered != PROTOCOL_CASES | {NATIVE_CASE}:
            raise RuntimeError("Vendor discovery differs from the exact mandatory ten protocols and native case")
        for phase, selector, count in [
            ("vendor-reconnect", "pd::retry::reconnect_regression_tests::", 8),
            ("vendor-retained", "pd::retry::test::", 2),
        ]:
            output = self.command(phase, vendor + [selector])
            require_cases(output, {case for case in PROTOCOL_CASES if case.startswith(selector)}, count)
        self.runtime = OwnedRuntime(self.artifacts / "ordinary", self.run_id + "-ordinary", 1, True)
        try:
            self.runtime.start()
            environment = {
                **self.environment,
                "TITO_NATIVE_PD_URI": self.runtime.endpoints[0].removeprefix("http://"),
                "TITO_NATIVE_RUN_ID": self.runtime.run_id,
            }
            output = self.command("tito-all-targets", ["cargo", "test", "--locked", "--offline", "--all-targets"], environment)
            require_cases(output, TITO_NATIVE_CASES)
        finally:
            self.runtime.close()
            self.runtime = None
        self.runtime = OwnedRuntime(self.artifacts / "leadership", self.run_id + "-transfer", 3, False)
        try:
            self.runtime.start()
            environment = {
                **self.environment,
                "PD_ADDRS": ",".join(self.runtime.endpoints),
                "TITO_NATIVE_RUN_ID": self.runtime.run_id,
            }
            output = self.command("vendor-native", vendor + [NATIVE_CASE, "--", "--exact"], environment, 90)
            require_cases(output, {NATIVE_CASE}, 1)
        finally:
            self.runtime.close()
            self.runtime = None
        print(f"Complete Tito verification passed; evidence: {self.artifacts}", flush=True)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt


def main():
    os.umask(0o077)
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
        verification = Verification(root)
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
