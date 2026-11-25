# Complete Tito verification

From the Tito checkout, run:

```sh
python3 scripts/verify.py --pd /path/to/pd-server --tikv /path/to/tikv-server
```

This is one verification command. It builds all Tito targets, then runs every Tito test on a PD
and a TiKV of its own. Tito depends on `tikv-client` 0.4.0 from crates.io, whose own tests are not
part of this command.

The command needs Python 3, the existing Rust toolchain through the normal `cargo` proxy, all locked
Cargo dependencies already cached, and the PD and TiKV v7.1.6 binaries of this machine, named by
`--pd` and `--tikv` or by `TITO_VERIFY_PD` and `TITO_VERIFY_TIKV`. It needs no Docker. The wrapper
refuses a path that is missing, not executable or whose `--version` does not name 7.1.6 before it
builds anything. On the owner's laptop the Server's test runner keeps checksum-verified binaries
under `server/target/host-tools/pd-v7.1.6-darwin-arm64/pd-server` and
`server/target/host-tools/tikv-v7.1.6-darwin-arm64/tikv-server`; the wrapper downloads nothing.

Tito's `Cargo.lock` is required. Every Cargo phase uses the same checkout, four build jobs and
`--locked --offline`. The wrapper does not resolve new dependency versions or select a replacement
toolchain. Missing prerequisites fail. Cargo uses the existing Tito `target/` cache. An explicitly
declared absolute `CARGO_TARGET_DIR` is honored for a reviewed private diagnostic assembly; the
wrapper never silently changes the Cargo test profile.

The wrapper raises its open-file limit to 16,384 when the hard limit allows, as the Server's test
runner does, and builds before starting a database. Its runtime is one PD and one TiKV
process on loopback ports it reserves, with their data in a new temporary folder and TiKV started
with the small settings the Server's host tests use (no reserved space, a 512 MiB block cache, 1,024
open raft files and small raft-engine files). It supplies the existing `TITO_NATIVE_PD_URI` and
`TITO_NATIVE_RUN_ID` declarations to the complete `cargo test --locked --all-targets` phase, where
every Tito test runs on that PD and TiKV: the library tests of models, indexes and the queue
(`src/tito_tests/`, `src/queue/`) and the native cases in `tests/`. There is no in-memory stand-in,
and a test checks results by reading TiKV after the commit, never trips or the keys a path read.
Every native case is mandatory:

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
- a key a batch read did not find is answered from memory while the lock of a prewrite that failed
  on its primary sits on that key and a fresh read meets the lock; a batch read leaves its
  transaction's own writes, deletes and locks as they were; a `create` and an `update` send no
  read: each returns while such a lock sits on its row; each case then removes the lock with the
  client's lock cleanup;
- a note whose key holds a live lock waits for it as a single read does: with the locks of a
  prewrite that failed on a third key left on its primary and on the record key, a plain client
  batch read gives up while the lock lives, and the note is answered only after the lock's TTL has
  passed and the transaction was rolled back.

Each invocation owns a unique `run-tito-...` identity, its processes, its temporary data folder and
its dynamically reserved loopback ports. It never reads application environment files, prepares a
retained Server runtime or touches a process it did not start; it starts every database process
with an empty environment. On success, refusal, failure or interruption it stops the active test
process group, stops its own database processes (SIGTERM, then SIGKILL after 30 seconds), removes
their data folder and verifies that none of its processes runs and the folder is gone. Each
process's log, source hashes, phase results and cleanup receipts stay under
`target/native-test-runs/<run>/`. New run directories are private `0700`, and generated artifacts
are private `0600`.

All source, manifests and locks must remain held for the entire command. This verifies Tito;
application recovery, Linux release artifacts and consumer browser suites
remain their own complete canonical gates against the exact reviewed Tito revision.
