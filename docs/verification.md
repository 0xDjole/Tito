# Complete Tito verification

From the Tito checkout, run:

```sh
python3 scripts/verify.py
```

This is one verification command. It builds and runs all Tito targets, explicitly discovers and
runs the vendored PD owner's eight reconnect regressions and two retained retry controls, then
runs the actual three-member PD leadership-transfer witness. Cargo does not run dependency unit
tests as part of Tito's `--all-targets`; the explicit owner phases are required.

The command needs Python 3, the existing Rust toolchain through the normal `cargo` proxy, Docker,
all locked Cargo dependencies already cached, and these exact cached images:

| Runtime | Pinned image |
| --- | --- |
| PD | `pingcap/pd:v7.1.6@sha256:59c9896b8040bc183ef75f28e2d7972deeccf1c682336e3c9e9406e629c40dcc` |
| TiKV | `pingcap/tikv:v7.1.6@sha256:6b07644a3378d98a5069fb9036399640861a1b7a0458efec4a562fcc6212b890` |

Both the Tito lock and the standalone vendored owner lock are required. Every Cargo phase uses
the same checkout, four build jobs and `--locked --offline`. The wrapper does not fetch an image,
resolve new dependency versions or select a replacement toolchain. Missing prerequisites fail.
Both Cargo packages share the existing Tito `target/` cache. An explicitly declared absolute
`CARGO_TARGET_DIR` is honored for a reviewed private diagnostic assembly; the wrapper never
creates a separate vendor target cache or silently changes the Cargo test profile.

The wrapper builds before starting a database. Its first disposable runtime has one real PD and
one real TiKV, and supplies the existing `TITO_NATIVE_PD_URI` and `TITO_NATIVE_RUN_ID` declarations
to the complete `cargo test --locked --all-targets` phase. Every native case is mandatory:

- the actual secondary-lock recovery and atomic batch replacement cases;
- 40 records of 1 MiB, more than the client's 32 MiB decoding budget, read in one forward scan,
  one reverse scan, ordinary index pages (each reads its entry keys, then its records in one batch
  get) and one batch get, and 16 queue events of almost 1 MiB pulled, completed and cleared;
- explicitly selected reference entries hold no record data: a record of 1 MiB with five ordinary index entries and one
  unique entry, where get, scan and batch get return an empty value for every ordinary entry and
  get returns the owner's id for the unique entry, while an index list and the unique lookup
  return the record, `execute_ids` returns its id, and its remove deletes every entry;
- eight mixed-mode cases verify exact copied-record and reference bytes, forward and reverse pages,
  cursors and key-only IDs, copy reads without primary records, malformed or mismatched copies,
  refresh after non-indexed edits, timestamps, conditional keys, rollback, removal, concurrent unique
  claims, batch ownership swaps, guards and same-schema restore with preserved versions and exact
  primary bytes for floating-point records. Record reads reject non-empty ordinary reference values,
  while key-only ID reads still return their keys without reading those values;
- a cluster-worker and a standalone-worker handler each complete their own pending queue row inside
  the transaction that applies their change, and the later acknowledgement changes nothing; two
  deliveries of one row conflict at commit and the retry skips, so the change applies once; a
  rolled-back handler transaction leaves the row and its owner index pending for redelivery; an
  undelivered event, a completed-row key and another event's pending row are refused without writes;
- guards on fence keys, which Tito only deletes (in TiKV a delete is a write: it leaves a Delete
  entry in the key's history until garbage collection, and reads never see it): a read by id in a
  transaction that writes (`get`, `get_many`, `get_versioned`, a hit of `find_one_by_unique_index`
  and `get_exclusive`) conflicts at commit with a save of its record, and a later versioned read
  returns the new version, while index reads (lists, which read their records in one batched read,
  and `execute_ids`, which reads keys only), scans and reads that find nothing add no guard; of
  480 pairs of transactions reading the same twenty records, 4 to 75 conflict; two exclusive reads
  of one record always conflict and the retry passes; a shared and an exclusive read of one record
  commit together in either order; a save and a remove conflict with shared and exclusive reads in
  either commit order; a transaction that only reads by id adds no guard, while `get_exclusive`
  counts as a write; and a retried transaction starts without the guards of the attempt before;
- ids held by other records: a new holder that reads the record it names and a delete of that
  record that checks the holders' index in its own transaction conflict in either commit order,
  and the retry refuses the delete or finds the record gone;
- two concurrent claims of one unique value conflict and the retry is refused, while values that
  differ only by case or spacing coexist and are found exactly.

After verifying that runtime's complete cleanup, the wrapper starts three PD members with distinct
native loopback client and peer ports.
The members share their owned container's network namespace, matching the existing Server test
runtime pattern. Their advertised addresses are the actual published loopback addresses; no
response rewriting or transport fault simulator is involved. Separate phases keep the maximum
database footprint lower than running TiKV alongside all three PD members.

The PD case reads actual membership and its current leader, selects a different live member,
and performs the native named leadership transfer. It first proves healthy idle does not replace
the connection. It then creates a real fresh connection on the same retained client and requires
the original timestamp worker's exact closed-channel failure inside the unchanged one-second
throttle. Its first recovery request must also begin inside that interval. A missed interval or
worker that stays open fails the case. The same client must return 256 distinct real timestamps
above every earlier observed timestamp, followed by another strictly greater timestamp. The
native membership must retain the same cluster identity and show the selected member as both PD
and etcd leader. No application transaction callback or business write is replayed by this proof.

Each invocation owns unique `run-tito-...` container names, labels, volumes, data and dynamically
reserved loopback ports. It never reads application environment files, prepares a retained Server
runtime or modifies an unrelated container. On success, refusal, failure or interruption it stops
the active test process group, captures its own native logs, verifies ownership before removing
only its resources, and verifies no resources with that exact owner label remain. Logs, source
hashes, phase results and cleanup receipts stay under `target/native-test-runs/<run>/`.
New run directories are private `0700`, and generated artifacts are private `0600`.

All source, manifests and locks must remain held for the entire command. This verifies Tito and
its vendored PD owner; application recovery, Linux release artifacts and consumer browser suites
remain their own complete canonical gates against the exact reviewed Tito revision.
