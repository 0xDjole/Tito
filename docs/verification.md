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
`TITO_NATIVE_RUN_ID` declarations to the complete `cargo test --locked --all-targets` phase. Every
native case is mandatory:

- the actual secondary-lock recovery and atomic batch replacement cases;
- 40 records of 1 MiB, more than the client's 32 MiB decoding budget, read in one forward scan,
  one reverse scan, ordinary index pages and one batch get, and 16 queue events of almost 1 MiB
  pulled, completed and cleared;
- a record of 1 MiB with five ordinary index entries, one unique entry and three holder entries, a
  save of about 7 MiB, where get, scan and batch get return the record's bytes for every ordinary and
  unique entry and an empty value for every holder entry, while an index list and the unique lookup
  return the record, `execute_ids` returns its id, and its remove deletes every entry;
- a save of a 1 MiB record with 62 copies, about 63 MiB, commits, and one with 64 copies is refused
  with the save bound's message before it writes;
- index entries hold the record's bytes and holder entries hold nothing, across forward and reverse
  pages, cursors and key-only IDs; lists and the unique lookup return the copies with the primary
  deleted; malformed, mismatched and moved copies fail naming their key; a non-indexed edit refreshes
  every copy; changed and removed entries; concurrent unique claims and batch ownership swaps;
  a unique hit for share guards its owner while lists add no guard; holder entries answer `execute_ids` and
  refuse every record read and scan, whatever bytes they hold;
- a batch read that did not find a key answers a later single or batch read of it from memory while
  a live lock sits on that key and a fresh read meets the lock; a batch read leaves its transaction's
  own writes, deletes and locks as they were, and a locked key still conflicts with a later writer;
  a save after a batch read of its `save_keys` finishes while a lock sits on its row, so it sent no
  read;
- a cluster-worker handler completes its own pending queue row inside the transaction that applies
  its change, and the later acknowledgement changes nothing; two deliveries of one row conflict at
  commit and the retry skips, so the change applies once; a rolled-back handler transaction leaves
  the row and its owner index pending for redelivery; an undelivered event, a completed-row key and
  another event's pending row are refused without writes;
- guards on fence keys, which Tito only deletes (in TiKV a delete is a write: it leaves a Delete
  entry in the key's history until garbage collection, and reads never see it): a read for share or
  for update in a transaction that writes (`get_for_share`, `get_many_for_share`,
  `get_versioned_for_share`, a hit of `find_one_by_unique_index_for_share` and `get_for_update`)
  conflicts at commit with a save of its record, and a later versioned read returns the new version,
  while index reads (lists, which read the copies in their entries, and `execute_ids`, which reads
  keys only), scans and reads that find nothing add no guard; of 480 pairs of transactions reading the
  same twenty records, 4 to 75 conflict; two reads for update of one record always conflict and the
  retry passes; a read for share and a read for update of one record commit together in either order;
  of 128 pairs of writers that read one record with a plain read (`get`, `get_many` or
  `find_one_by_unique_index`, in turn), none conflicts, each plain read commits beside a save or a
  remove of its record, and a record the same transaction also reads for share or for update keeps
  that guard; a save and a remove conflict with reads for share and for update in either commit order;
  a transaction that only reads adds no guard, while `get_for_update` counts as a write; and a retried
  transaction starts without the guards of the attempt before;
- ids held by other records: a new holder that reads the record it names and a delete of that
  record that checks the holders' index in its own transaction conflict in either commit order,
  and the retry refuses the delete or finds the record gone;
- two concurrent claims of one unique value conflict and the retry is refused, while values that
  differ only by case or spacing coexist and are found exactly;
- a save whose batch read meets a live lock waits for it as a single read does: with a real
  secondary lock left on the record key by a failed prewrite, one plain client batch read gives up
  while the lock lives, and the save succeeds only after the lock's TTL has passed.

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
