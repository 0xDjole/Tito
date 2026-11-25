# Complete Tito verification

From the Tito checkout, run:

```sh
python3 scripts/verify.py --pd /path/to/pd-server --tikv /path/to/tikv-server
```

This is one verification command. It builds and runs all Tito targets, explicitly discovers and
runs the vendored PD owner's eight reconnect regressions and two retained retry controls and the
vendored transaction buffer's eight cases, then runs the actual three-member PD leadership-transfer
witness. Cargo does not run dependency unit tests as part of Tito's `--all-targets`; the explicit
owner phases are required.

The command needs Python 3, the existing Rust toolchain through the normal `cargo` proxy, all locked
Cargo dependencies already cached, and the PD and TiKV v7.1.6 binaries of this machine, named by
`--pd` and `--tikv` or by `TITO_VERIFY_PD` and `TITO_VERIFY_TIKV`. It needs no Docker. The wrapper
refuses a path that is missing, not executable or whose `--version` does not name 7.1.6 before it
builds anything. On the owner's laptop the Server's test runner keeps checksum-verified binaries
under `server/target/host-tools/pd-v7.1.6-darwin-arm64/pd-server` and
`server/target/host-tools/tikv-v7.1.6-darwin-arm64/tikv-server`; the wrapper downloads nothing.

Both the Tito lock and the standalone vendored owner lock are required. Every Cargo phase uses
the same checkout, four build jobs and `--locked --offline`. The wrapper does not resolve new
dependency versions or select a replacement toolchain. Missing prerequisites fail.
Both Cargo packages share the existing Tito `target/` cache. An explicitly declared absolute
`CARGO_TARGET_DIR` is honored for a reviewed private diagnostic assembly; the wrapper never
creates a separate vendor target cache or silently changes the Cargo test profile.

The wrapper raises its open-file limit to 16,384 when the hard limit allows, as the Server's test
runner does, and builds before starting a database. Its first runtime is one PD and one TiKV
process on loopback ports it reserves, with their data in a new temporary folder and TiKV started
with the small settings the Server's host tests use (no reserved space, a 512 MiB block cache, 1,024
open raft files and small raft-engine files). It supplies the existing `TITO_NATIVE_PD_URI` and
`TITO_NATIVE_RUN_ID` declarations to the complete `cargo test --locked --all-targets` phase, where
every Tito test runs on that PD and TiKV: the library tests of models, indexes and the queue
(`src/tito_tests/`, `src/queue/`) and the native cases in `tests/`. There is no in-memory stand-in,
and a test checks results by reading TiKV after the commit, never trips or the keys a path read.
Every native case is mandatory:

- the actual secondary-lock recovery case;
- 40 records of 1 MiB, more than the client's 32 MiB decoding budget, read through `scan_records`,
  index pages in both directions and `read_many`, and 16 queue events of almost 1 MiB pulled and
  acknowledged, their rows gone;
- the `Full`, `KeysOnly` and `Part` entries of a record of 1 MiB read back: every `Full` entry
  holds the record's bytes, every `KeysOnly` entry nothing and every `Part` entry its fields;
- a save just under `MAX_SAVE_BYTES` commits its copies, and a larger one is refused with the save
  bound's message before it writes;
- a cluster-worker handler acknowledges its own pending queue row inside the transaction that
  applies its change, and the later acknowledgement changes nothing; two deliveries of one row
  conflict at commit and the retry skips, so the change applies once; a rolled-back handler
  transaction leaves the row and its owner index pending for redelivery; an undelivered event, a
  row of another queue and another event's pending row are refused without writes;
- guards on fence keys, which Tito only deletes (in TiKV a delete is a write: it leaves a Delete
  entry in the key's history until garbage collection, and reads never see it): a note with
  `Guard::Share` or `Guard::Update` in a transaction that writes conflicts at commit with an update
  of its record, while index pages, scans and notes that find nothing add no guard; of 480 pairs of
  transactions reading the same twenty records for share, 4 to 75 conflict; two reads for update of
  one record always conflict and the retry passes; a read for share and a read for update of one
  record commit together in either order; writers that read one record with `Guard::None` never
  conflict, such a read commits beside an update or a remove of its record, and a record the same
  transaction also reads for share or for update keeps that guard; an update and a remove conflict
  with reads for share and for update in either commit order; a transaction that only reads adds no
  guard, while a read for update counts as a write; and a retried transaction starts without the
  guards of the attempt before;
- ids held by other records: a new holder that reads the record it names for share and a delete
  of that record that checks the holders' `KeysOnly` index in its own transaction conflict in either
  commit order, and the retry refuses the delete or finds the record gone;
- two concurrent creates of one unique value: one commits and the other ends in
  `UniqueViolation`, while values that differ only by case or spacing coexist and are found
  exactly;
- a key a batch read did not find is answered from memory while a live lock sits on that key and a
  fresh read meets the lock; a batch read leaves its transaction's own writes, deletes and locks as
  they were; a `create` and an `update` send no read: each returns while another transaction's live
  lock sits on its row;
- a note whose key holds a live lock waits for it as a single read does: with a real secondary lock
  left on the record key by a failed prewrite, a plain client batch read gives up while the lock
  lives, and the note is answered only after the lock's TTL has passed.

After stopping that runtime and removing its folder, the wrapper starts three PD processes with
distinct loopback client and peer ports in a new temporary folder. Their advertised addresses are
the actual loopback addresses; no response rewriting or transport fault simulator is involved.
Separate phases keep the maximum database footprint lower than running TiKV alongside all three PD
members.

The PD case reads actual membership and its current leader, selects a different live member,
and performs the native named leadership transfer. It first proves healthy idle does not replace
the connection. It then creates a real fresh connection on the same retained client and requires
the original timestamp worker's exact closed-channel failure inside the unchanged one-second
throttle. Its first recovery request must also begin inside that interval. A missed interval or
worker that stays open fails the case. The same client must return 256 distinct real timestamps
above every earlier observed timestamp, followed by another strictly greater timestamp. The
native membership must retain the same cluster identity and show the selected member as both PD
and etcd leader. No application transaction callback or business write is replayed by this proof.

Each invocation owns a unique `run-tito-...` identity, its processes, its temporary data folders and
its dynamically reserved loopback ports. It never reads application environment files, prepares a
retained Server runtime or touches a process it did not start; it starts every database process
with an empty environment. On success, refusal, failure or interruption it stops the active test
process group, stops its own database processes (SIGTERM, then SIGKILL after 30 seconds), removes
their data folder and verifies that none of its processes runs and the folder is gone. Each
process's log, source hashes, phase results and cleanup receipts stay under
`target/native-test-runs/<run>/`. New run directories are private `0700`, and generated artifacts
are private `0600`.

All source, manifests and locks must remain held for the entire command. This verifies Tito and
its vendored PD owner; application recovery, Linux release artifacts and consumer browser suites
remain their own complete canonical gates against the exact reviewed Tito revision.
