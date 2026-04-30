# CLAUDE.md

Handoff for future sessions working on this repo.

## What this is

A benchmark / reference implementation for **incremental RocksDB checkpoints to an
object store, with a Kafka-driven build pipeline and shard-aware StatefulSet
topology**. Two artifacts matter long-term:

- **`checkpoint-core`** — the portable library. Manifest contract, store
  abstraction (local FS + S3), checkpoint manager (build/upload/diff/GC),
  checkpoint restorer (parallel download), shard assignment, bandwidth limiter.
- **`state-builder`** — Kafka → RocksDB consumer that drives checkpoints on a
  cadence. The reference pattern for hooking `checkpoint-core` into a
  source-of-truth feed.

The other two modules are **PoC-only** and won't move forward:

- **`feature-server`** — restores from store, optionally tails Kafka, serves
  HTTP. Demonstrates round-trip restore + serve. Covered briefly below.
- **`bench-producer`** — synthetic Kafka producer for the benchmark feed.
  Covered briefly below.

When iterating, default focus is **`checkpoint-core` + `state-builder`**.
Treat the other two as scaffolding.

## Repo layout

```
checkpoint-core/                 (the library)
  Manifest, SstEntry, MetaEntry  — JSON-serializable record types
  CheckpointStore                — abstract interface (AutoCloseable)
  LocalFsCheckpointStore         — files in a directory
  S3CheckpointStore              — async S3 + TransferManager (multipart)
  CheckpointStores               — fromEnv() factory selecting backend
  CheckpointManager              — build / diff / upload / GC orchestrator
  CheckpointRestorer             — parallel download orchestrator
  ShardAssignment                — pure value: ownership math + path segment
  OffsetSupplier                 — single-method seam: Map<String,Long>
  BandwidthLimiter               — token-bucket on bytes (acquire(n) blocks)
  Metrics                        — JSON-line structured events

state-builder/
  Main, Config                   — env-driven wiring
  RocksDbStore                   — RocksDB open + WriteBatch helper
  KafkaConsumerLoop              — assign() + write + checkpoint trigger

feature-server/                  (PoC)
  Main, Config, Restorer wiring, KafkaTailer, FeatureHttpServer

bench-producer/                  (PoC)
  Main, SyntheticRecordGenerator

scripts/local.sh                 — one-command stack (up/down/status/logs/clean/restart)
docker-compose.yml               — KRaft Kafka + topic-init + MinIO + bucket-init
```

Java 21, Maven. `mvn -DskipTests package` builds shaded uber-jars in each
module's `target/`.

## Build & run

```
mvn -DskipTests package          # build
scripts/local.sh up              # full local stack: kafka + minio + 3 shards
scripts/local.sh logs builder-0  # follow any process log
scripts/local.sh down            # stop java + docker
scripts/local.sh clean           # remove data/ logs/ pids/
```

`scripts/local.sh up` produces 200k records, spawns `SHARD_COUNT=3` builders
(each in `data/builder-N/`), waits for the first checkpoint to commit on each,
then spawns one feature-server per shard on ports 8080–8082.

For S3 (MinIO) instead of local FS:

```
CHECKPOINT_STORE_TYPE=s3 \
S3_BUCKET=rocksdb-checkpoints \
S3_ENDPOINT_URL=http://localhost:9000 \
S3_PATH_STYLE=true \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
scripts/local.sh up
```

For real AWS S3: drop `S3_ENDPOINT_URL` and `S3_PATH_STYLE`, point
credentials at the target account.

---

## checkpoint-core (deep)

### The data contract: `Manifest`

A manifest is a JSON document, one per checkpoint version, persisted at
`<root>/<shard-prefix>/manifests/<20-zero-padded-version>.json`:

```
{
  "version": 7,
  "previousVersion": 6,
  "createdAtMs": 1714200000000,
  "offsets": { "feature-events:0": 12345, "feature-events:3": 67890, ... },
  "ssts": [ { "name": "000123.sst", "sizeBytes": 67108864, "sha256": "..." }, ... ],
  "metaFiles": [ { "name": "CURRENT", "sizeBytes": 16 }, ... ],
  "totalSizeBytes": 1234567890
}
```

Three things to internalize:

1. **`offsets` is the resume contract.** Keys are `"topic:partition"` strings
   pointing to the *next offset to consume*. Whatever the source feed is, the
   manifest's offset map is what tells a restorer where to pick up. If you port
   this code, this is the seam.
2. **`ssts` deduplicate across versions; `metaFiles` are versioned per
   checkpoint.** SSTs are immutable in RocksDB — once `000123.sst` exists, its
   bytes never change — so we diff by name and only upload new SSTs. Meta
   files (CURRENT, MANIFEST-*, OPTIONS-*, IDENTITY) change every checkpoint and
   are tiny, so we re-upload them all under `meta/v<N>/`.
3. **`totalSizeBytes` is for monitoring.** Don't trust it for restore math —
   sum from the entries.

Manifest is a Java `record` and round-trips through Jackson via the canonical
constructor (Jackson 2.12+ supports records natively, no annotations needed).
**Schema bump = breaking change.** If you add a field, either default it on
read or version the format explicitly.

### Store layout

Identical between local FS and S3 (same paths, same names):

```
<root>/[<shard-prefix>/]
  manifests/<version>.json          one per checkpoint version
  ssts/<name>.sst                   pool of immutable SSTs (deduplicated)
  meta/v<N>/{CURRENT,MANIFEST-...,OPTIONS-...,IDENTITY}    per-version meta
```

`<shard-prefix>` is `shard-NNNNN-of-MMMMM` when `SHARD_COUNT > 1`, empty
otherwise. **Both `id` and `count` are baked into the path on purpose** — bumping
`SHARD_COUNT` produces different paths so resharding can't silently corrupt
in-place. Resharding requires a fresh build; this is intentional.

For S3, `<root>` is the bucket + optional `S3_PREFIX`. For local FS,
`<root>` is `CHECKPOINT_STORE_DIR`.

### `CheckpointStore` interface

```java
void putSst(String name, Path local)
void downloadSst(String name, Path target)
void deleteSst(String name)
Set<String> listSsts()

void putMetaFile(long version, String name, Path local)
void downloadMetaFile(long version, String name, Path target)
void deleteMetaVersion(long version)

void putManifest(Manifest m)
Optional<Manifest> getManifest(long version)
Optional<Manifest> getLatestManifest()
List<Long> listManifestVersions()             // sorted ascending
void deleteManifest(long version)

default void close() {}                         // AutoCloseable
```

This is the seam for swapping backends. Any new store impl that satisfies this
interface drops in via `CheckpointStores.fromEnv` (add a `case` branch).

`putSst` is **idempotent on name**: SSTs are immutable so a re-put is harmless.
The local impl skips writes when the file already exists; the S3 impl just
overwrites (cheap; same content).

### `CheckpointManager` — the build side

Lifecycle:

- Constructor loads `getLatestManifest()` from the store. If present, it
  becomes `prevManifest` and the next checkpoint version is `prevManifest.version
  + 1`. This is how restart-resumption works.
- `checkpoint()` is the only public operation. **Single-flight** — sequential
  call, blocks the caller until the manifest is committed.

`checkpoint()` flow:

```
1. Capture offsets via offsetSupplier.currentOffsets()  (ms, depends on supplier)
2. Checkpoint.createCheckpoint(stagingDir/v<N>)         (RocksDB hard-links SSTs, flushes memtable)
3. Walk staging: classify .sst vs meta vs LOCK
4. Diff SSTs by name vs prevManifest.ssts               (microseconds)
5. Parallel upload: new SSTs + all meta files            (the long pole)
   - each upload acquires bandwidth budget if limiter set
6. Build full manifest (reused entries carry over for sha256)
7. store.putManifest(m)                                  (atomic via temp+rename / S3 PUT)
8. prevManifest = m; delete stagingDir/v<N>
9. GC: drop manifests beyond retainLastN, remove SSTs not in any retained set
10. metrics.checkpointCreated(...)
```

**Tradeoffs locked in here:**

- **Single-flight, sequential.** Next checkpoint can't start until previous
  upload completes. Correct (no concurrent createCheckpoint, no diff race) but
  the consumer thread blocks for the upload window. For benchmark this is
  fine. For prod fanout, you'd want a queue between createCheckpoint and
  upload — see "Open work."
- **SSTs diffed by name, not content.** Safe because RocksDB never reuses an
  SST filename for different content. Don't change this without understanding
  why.
- **`Files.copy` from staging to local store, not hard-link.** Local FS impl
  copies, which is wasted I/O for the local case but matches the S3 path's
  bandwidth profile. Symmetric for benchmarking.
- **SHA-256 is optional** (`computeChecksums`, default true). Costs a few
  hundred MB/s per file (one core); turn it off if benchmark numbers indicate
  it's the bottleneck. Currently only stored, not verified.
- **`Checkpoint.createCheckpoint` flushes memtable.** Not optional. Means a
  busy memtable extends `buildMs`.
- **Staging dir must be on the same filesystem as the live DB**, otherwise
  `createCheckpoint` falls back to copy instead of hard-link and is slow.
- **GC retention is by manifest count, not time.** `retainLastN=10` means we
  always keep the last 10 manifests. Time-based retention is left to S3
  lifecycle rules (carefully — make sure they don't fight the GC).

### `CheckpointRestorer` — the load side

Mirror image. Given a manifest, parallel-download all SSTs + all meta files for
that version into a target dir, optionally rate-limited.

- Reads via `CheckpointStore.downloadSst` and `downloadMetaFile`.
- Each task acquires bandwidth budget before starting if a `BandwidthLimiter`
  is wired.
- Files written to `target.toPath().resolve(<name>.part)` then atomic-moved to
  the final name (so a crash mid-download doesn't leave a half-file that
  RocksDB tries to open).
- Emits `checkpoint_restored` metric event on completion.

### `ShardAssignment`

Pure value record `(int shardId, int shardCount)`. The whole sharding model is
collapsed into this:

- `ownsPartition(p)` — `p % shardCount == shardId`. Round-robin modulo.
- `ownedPartitions(total)` — list of partitions this shard owns.
- `pathSegment()` — `"shard-NNNNN-of-MMMMM"`.
- `assertOwns(Manifest)` — throws if the manifest contains offsets for
  partitions this shard doesn't own. Defensive check on restore.
- `fromEnv()` — reads `SHARD_COUNT` and `SHARD_ID` (or derives `SHARD_ID`
  from the trailing ordinal of `HOSTNAME` mod `SHARD_COUNT`).

The derivation rule **works for both StatefulSets**: a builder set with N
replicas has ordinals 0..N-1 (mapped 1:1 to shard ids), and a server set with
K×N replicas has ordinals 0..KN-1 mapped to shard ids via mod-N (multiple
servers per shard).

If your env has different env var names, use the overloads:
`ShardAssignment.fromEnv(idVar, countVar)` or
`fromEnv(idVar, countVar, hostnameVar)`. Or construct directly from any source
of values: `new ShardAssignment(id, count)`. The Config classes in the apps
are the only place env-var names appear; core stays neutral.

### `BandwidthLimiter`

Token bucket on bytes. Capacity defaults to 2× refill rate. `acquire(bytes)`
blocks the calling thread (synchronized + sleep, no notify needed) until the
budget allows; `parseRate("1.5G")` accepts `K`/`M`/`G` (binary) suffixes.

**It's a soft cap.** Pacing happens at file-acquire time before each
download/upload. Once a file's bandwidth is acquired, the actual transfer runs
at full speed (TM may use multipart concurrency internally). Average rate over
many files tracks the limit; instantaneous can briefly exceed. For "X% of
NIC" goals this is fine. A strict instantaneous cap requires a custom
`AsyncResponseTransformer` and is not worth the complexity for this workload.

---

## state-builder (deep)

### Threading model

Single producer thread (`consumer-loop`) does everything in the data path:

```
poll → write batch to RocksDB → maybe trigger checkpoint
```

- The consumer is **not thread-safe**. Anything that touches it
  (`partitionsFor`, `assign`, `position`, `seek`, `poll`) runs on this thread.
- Checkpoints are triggered **between polls**. The single-flight property means
  the consumer pauses until the upload completes. With
  `MAX_POLL_INTERVAL_MS=1200000` (20 min default), this is generous; for very
  long uploads (TB-scale) bump it further.
- Uploads run on a **separate fixed thread pool** owned by the
  `CheckpointManager` (`uploadParallelism`, default 8). Upload tasks are
  fire-and-join via `CompletableFuture` from the consumer thread.
- The `OffsetSupplier` (`KafkaConsumerLoop` itself) is called from the
  consumer thread during checkpoint, so `consumer.assignment()` and
  `consumer.position(tp)` are safe.

### Partition assignment — **`assign()`, not `subscribe()`**

This is the most important locked-in tradeoff. `KafkaConsumerLoop.run()`:

```
1. consumer.partitionsFor(topic) per topic        → discovers total partitions
2. shard.ownedPartitions(total) per topic          → deterministic slice
3. consumer.assign(<TopicPartition list>)          → no consumer group involved
4. shard.assertOwns(latestManifest) if any          → fail loudly on shard drift
5. Seek each owned TP to manifest offset (or seekToBeginning if no entry)
6. Main poll loop — unchanged
```

Why `assign()`:

- StatefulSet ordinal == shard identity. Sticky pod identity → sticky partition
  ownership. No rebalancing surprises on restart.
- Removes the group coordinator from the critical path.
- Offsets live in **our** manifest, not `__consumer_offsets`. We never commit
  to Kafka.

`KAFKA_GROUP_ID` is still set as a label for ops/observability but no longer
drives anything functionally. **Do not revert to `subscribe()`** — it
fundamentally breaks the shard model.

### Checkpoint cadence

Two thresholds, OR'd:

- `CHECKPOINT_INTERVAL_MS` — default 60_000 (60s).
- `CHECKPOINT_MAX_RECORDS` — default 1_000_000.

For local benchmarks the script overrides to 15s / 50k for snappier iteration.
For prod, tune based on:

- **How much do you tolerate replaying after a builder restart?** Lower
  interval = less replay.
- **How much SST churn are you willing to upload?** Frequent checkpoints =
  more partial-uploaded SSTs. The diff helps, but compaction churn at small
  intervals can dominate.
- **S3 PUT cost.** Trivial at this scale (cents/day) but worth knowing.

### RocksDB tuning (defaults)

`RocksDbStore` opens with:

- LZ4 compression
- `bytes_per_sync = 1 MiB` (smooths page cache flushes during writes)
- `write_buffer_size = 64 MiB`, `max_write_buffer_number = 4`
- `target_file_size_base = 64 MiB`
- `level_compaction_dynamic_level_bytes = true`
- `WriteOptions.disableWAL = true` (Kafka is the source of truth — WAL is
  redundant and slows ingest)

Tuned for ingest speed on the assumption that loss of in-memtable data on
crash is recoverable from Kafka. **If you re-enable WAL, you also need to ship
`*.log` files in `metaFiles`** — currently the manager treats anything not
ending in `.sst` as a meta file, so WAL files would automatically be included,
but this hasn't been validated end-to-end.

For 1 TB scale, you'll likely want bigger block cache (default RocksDB block
cache is 8 MiB — way too small), more write buffers, and ZSTD compression for
deeper levels. Knob in `RocksDbStore.java`.

---

## Numbers / expectations

Rough back-of-envelope figures for sanity-checking benchmark output. Assume
i4i-class hardware (NVMe local disk, 25 Gbit NIC) and same-region S3.

| Phase                    | Dominated by                  | Expected for 1 TB DB / 1 GB delta |
|--------------------------|-------------------------------|------------------------------------|
| `createCheckpoint`        | memtable flush + manifest fsync| ~100 ms to a few seconds          |
| Diff (set difference)    | nothing                       | microseconds                       |
| Upload (incremental)     | bandwidth                     | 1 GB / ~3 GB/s ≈ 0.3–1 s          |
| Upload (full first time) | bandwidth                     | 1 TB / ~2.5 GB/s ≈ ~7 minutes     |
| Manifest PUT             | one S3 round-trip             | ~10–30 ms                          |
| GC                       | list + deletes                | seconds                            |
| Restore (full)           | bandwidth                     | 1 TB / ~2.5 GB/s ≈ ~7 minutes     |
| Single feature lookup    | RocksDB block cache           | ~50 µs hot, ~1–3 ms cold (NVMe)   |

Sizing rules of thumb:

- **NVMe needed**: `1.5–2× DB size` (live DB + staging hard links + compaction
  overhead).
- **Memory**: block cache (tune to ~25–40% of system RAM for hot working
  set), plus `write_buffer_size × max_write_buffer_number` per CF.
- **S3 storage**: `bytes_per_checkpoint × retainLastN`. With dedup of immutable
  SSTs, this is roughly `1× DB size + small delta × N`. ~$23/TB-month standard.
- **S3 request cost**: PUT/LIST/DELETE all in cents. Don't optimize for this.
- **NIC**: cap restore at 50–70% via `RESTORE_BANDWIDTH_BYTES_PER_SEC` to leave
  headroom for sidecars and request traffic.

---

## Configuration surface (env vars)

Defaults shown; all overridable via env or `-D` system properties. Apps read
from env first, then system properties. Empty/blank treated as unset.

**Sharding** (read by core, both apps):

| Var | Default | Notes |
|-----|---------|-------|
| `SHARD_COUNT` | `1` | total shards in the deployment |
| `SHARD_ID` | derived from `HOSTNAME` mod `SHARD_COUNT` | explicit overrides derivation |
| `HOSTNAME` | OS-provided | derivation parses trailing digits |

**Store backend** (read by core):

| Var | Default | Notes |
|-----|---------|-------|
| `CHECKPOINT_STORE_TYPE` | `local` | `local` or `s3` |
| `CHECKPOINT_STORE_DIR` | `./data/checkpoints` | local-only |
| `S3_BUCKET` | (required for s3) | |
| `S3_PREFIX` | `""` | optional sub-prefix |
| `S3_ENDPOINT_URL` | (unset) | for MinIO/LocalStack |
| `S3_PATH_STYLE` | `true` | path-style addressing (MinIO needs it) |
| `S3_MIN_PART_BYTES` | `8388608` | TM multipart minimum |
| `AWS_REGION` | `us-east-1` | |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | default chain | |

**state-builder**:

| Var | Default | Notes |
|-----|---------|-------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | |
| `KAFKA_TOPICS` | `feature-events` | comma-separated |
| `KAFKA_GROUP_ID` | `state-builder` | label only — `assign()` is used |
| `KAFKA_CLIENT_ID` | `state-builder-1` | |
| `KAFKA_MAX_POLL_RECORDS` | `5000` | |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `1200000` | bump if uploads run longer |
| `ROCKSDB_DATA_DIR` | `./data/rocksdb` | per-shard if running multiple |
| `ROCKSDB_DISABLE_WAL` | `true` | Kafka is source of truth |
| `CHECKPOINT_STAGING_DIR` | `./data/staging` | must be same FS as data dir |
| `CHECKPOINT_INTERVAL_MS` | `60000` | time threshold |
| `CHECKPOINT_MAX_RECORDS` | `1000000` | record-count threshold |
| `CHECKPOINT_RETAIN_LAST_N` | `10` | manifests kept after each commit |
| `CHECKPOINT_UPLOAD_PARALLELISM` | `8` | upload thread pool size |
| `CHECKPOINT_COMPUTE_CHECKSUMS` | `true` | SHA-256 of new SSTs |
| `UPLOAD_BANDWIDTH_BYTES_PER_SEC` | `0` | `0` = unlimited; `1.5G` ok |

**feature-server** (PoC):

| Var | Default | Notes |
|-----|---------|-------|
| `RESTORE_DIR` | `./data/restored-rocksdb` | |
| `RESTORE_VERSION` | `-1` | `-1` = latest |
| `RESTORE_PARALLELISM` | `8` | download thread pool size |
| `RESTORE_BANDWIDTH_BYTES_PER_SEC` | `0` | `0` = unlimited |
| `HTTP_PORT` | `8080` | |
| `HTTP_THREADS` | `16` | |
| `TAIL_KAFKA` | `false` | apply post-checkpoint updates |
| `KAFKA_*` | (same as builder) | only used when tailing |

**bench-producer** (PoC):

| Var | Default |
|-----|---------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `KAFKA_TOPIC` | `feature-events` |
| `TOTAL_RECORDS` | `1000000` |
| `KEY_CARDINALITY` | `100000` |
| `TARGET_RATE_PER_SEC` | `0` (unbounded) |
| `VALUE_FIELD_COUNT` | `100` |
| `VALUE_STRING_LEN` | `20` |

---

## Topology — how it's supposed to work in production

Two StatefulSets:

- **`state-builder`** with `B` replicas. Each pod owns 1/B of the partitions
  (deterministic by ordinal). Each writes to its own RocksDB (NVMe local disk)
  and uploads checkpoints to `<store>/shard-X-of-B/`.
- **`feature-server`** with `S` replicas, where `S = B × K` (K servers per
  shard for HA / read fan-out). Server ordinal `j` serves shard `j mod B` —
  i.e., `j=0..S-1` maps to shard ids `0,1,...,B-1,0,1,...`. Each restores from
  `<store>/shard-{j mod B}-of-B/` and serves features for that shard.

A separate **Kafka → HDFS dump** runs in parallel, persisting the full record
stream for AR diagnostic replay. Our manifest's offset map is *authoritative*
for "where to resume from after restoring v_N"; the HDFS dump is the long-tail
archive (months back). Manifest retention can stay short (last N for failover)
without losing diagnostic capability.

Resharding is **explicitly out of scope of the API**. Bumping `SHARD_COUNT`
produces different paths (`shard-2-of-3` vs `shard-2-of-4`), forcing a fresh
build under the new prefix. Don't try to hot-reshard.

### Sticky vs ephemeral

- Builder pods are stateful for two reasons: (a) sticky partition ownership
  (no group rebalance), (b) catching up from Kafka is cheaper from existing
  RocksDB than from a checkpoint restore.
- Server pods are stateful for one reason: their RocksDB lives on local disk
  and is expensive to rebuild. Restore-from-checkpoint is the bootstrap path;
  tailing Kafka keeps it fresh.

---

## Porting guide

To use this in another project (different ingestion model, same checkpoint
mechanics), the seam to plug into is `checkpoint-core`.

What to take:
- All of `checkpoint-core` (one Maven module). Currently zero Kafka deps.
  Optional: keep S3 if you want it; otherwise just `LocalFsCheckpointStore`.

What to replace:
- `state-builder/KafkaConsumerLoop` is the *example* of how to drive
  `CheckpointManager` from a feed. In a different project, write your own
  driver that:
  1. Consumes records from your source (a queue, log files, gRPC, whatever).
  2. Writes them to a `RocksDB` you opened.
  3. Implements `OffsetSupplier`: a `Map<String, Long>` representing
     "where am I now?" using whatever resumability marker your source has
     (Kafka offsets, file offsets, sequence numbers, etc.).
  4. Calls `mgr.checkpoint()` on whatever cadence makes sense.

What to swap (optional):
- A new `CheckpointStore` impl for a different backend (HDFS, GCS, Azure
  Blob, custom HTTP-served local FS for the no-S3 design we discussed). One
  class. Add a `case` in `CheckpointStores.fromEnv` and you're done.

Glue you write in the consuming project:

```java
RocksDB db = RocksDB.open(opts, dataDir);
CheckpointStore store = ...your impl...;
ShardAssignment shard = ShardAssignment.fromEnv();   // or new ShardAssignment(...)

CheckpointManager mgr = CheckpointManager.builder()
    .db(db)
    .stagingRoot(stagingDir)
    .store(store)
    .offsetSupplier(() -> myCurrentOffsets())        // your seam
    .uploadParallelism(8)
    .retainLastN(10)
    .uploadBandwidthLimiter(new BandwidthLimiter(...))   // optional
    .build();

// On your cadence:
Manifest m = mgr.checkpoint();
```

To restore in a different project:

```java
try (CheckpointRestorer r = new CheckpointRestorer(store, parallelism, metrics, limiter)) {
    Manifest m = r.restoreLatest(targetDir);
    shard.assertOwns(m);                              // defensive
    RocksDB db = RocksDB.open(opts, targetDir.toString());
    // resume your feed from m.offsets()
}
```

The whole library has minimal third-party surface: `rocksdbjni`, Jackson,
SLF4J, and (for S3) AWS SDK v2 + s3-transfer-manager. No Spring, no Guava, no
Netty in the application code.

---

## Common iteration tasks

### "I want to change checkpoint cadence."

`CHECKPOINT_INTERVAL_MS` and `CHECKPOINT_MAX_RECORDS` envs. Or change defaults
in `state-builder/Config.java`.

### "I want to add a new store backend."

1. Implement `CheckpointStore` (10 methods + close).
2. Add a `case` to `CheckpointStores.fromEnv` reading whatever env vars your
   backend needs.
3. Done. State-builder and feature-server pick it up via the factory.

### "I want a different offset format."

Manifest `offsets` is `Map<String, Long>` — the keys are opaque strings. The
Kafka driver uses `"topic:partition"` but the core doesn't care. Use whatever
resume marker your feed has. The `assertOwns(Manifest)` helper assumes the
key suffix after the last `:` is a parseable int (the partition); if you use a
different key format, override or skip that check.

### "I want to drop S3 and pull peer-to-peer."

Implement `HttpClientCheckpointStore` against the same interface; serve files
from the builder via JDK `HttpServer`. Same diff/upload/restore code paths
work unchanged. ~200 LOC.

### "I want to apply lifecycle rules in S3 instead of in-process GC."

Set `CHECKPOINT_RETAIN_LAST_N=0` to disable in-process GC and wire a
lifecycle rule on the bucket. **Make sure the lifecycle rule is by-prefix
aware** — you don't want to expire SSTs referenced by manifests still in the
retention window. Easier to keep in-process GC; harder to reason about
otherwise.

### "I want to validate SHA-256 on restore."

Currently `sha256` is stored on `SstEntry` but never checked. Add a check in
`CheckpointRestorer` after each download, before the atomic rename.

---

## Things NOT to do

- **Don't go back to `subscribe()`**. Sharding depends on `assign()` for
  determinism. Reverting silently breaks ownership.
- **Don't share the RocksDB data dir across processes.** RocksDB takes a
  process-exclusive `LOCK` file. Per-shard dirs are mandatory.
- **Don't store offsets in `__consumer_offsets`.** Manifest is authoritative.
  We don't `commitSync` to Kafka.
- **Don't auto-create the topic** with default partition count. The
  docker-compose `kafka-init` creates it explicitly with 8 partitions and
  compaction. In prod, do the same; auto-create gives you 1 partition and
  delete cleanup, neither of which matches the design.
- **Don't put the staging dir on a different filesystem from the live
  RocksDB.** `Checkpoint.createCheckpoint` falls back to copy and is
  catastrophically slow at TB scale.
- **Don't trust `totalSizeBytes` on the manifest** for restore math. Sum the
  entries.
- **Don't drop bucket capacity below 2× refill rate** on `BandwidthLimiter`
  without measuring stalls.
- **Don't expect resharding to work hot.** Bumping `SHARD_COUNT` requires a
  fresh build under the new prefix.

---

## Open work / known gaps

- **Producer-consumer queue between createCheckpoint and upload.** Today the
  consumer thread blocks for the upload window (single-flight). For
  large-scale prod, decouple: createCheckpoint is fast (hard links), upload
  can run async; the only constraint is the next checkpoint waits for the
  previous upload to complete (or accumulates SST diff state correctly).
  Worth doing if upload time becomes a bottleneck for ingest throughput.
- **WAL file handling not validated.** If you re-enable WAL,
  `Checkpoint.createCheckpoint` will produce `*.log` files; the manager will
  classify them as meta files (since they don't end in `.sst`) and upload
  them under `meta/v<N>/`. Restore would need to put them back in the right
  place. Not tested end-to-end.
- **No SHA-256 verification on restore.** Stored, not checked. Easy to add.
- **`Checkpoint.createCheckpoint` flushes memtable**, but we don't call
  `flush(WriteOptions.setWaitForFlush(true))` ourselves. Should be fine —
  `createCheckpoint` does the right thing — but this hasn't been
  stress-tested under heavy write load.
- **Server tailer doesn't checkpoint.** It applies updates but never persists
  them as new checkpoints; on restart it starts over from the latest builder
  manifest. Fine for the PoC; not a long-term position.
- **`feature-server` wipes `RESTORE_DIR` on every start** to guarantee a
  clean slate. For prod fast-restart you'd want to keep the existing dir,
  diff against the latest manifest, and download only what's missing. Same
  diff-by-name logic as the builder, mirrored.

---

## Metrics

Two JSON-line events emitted via SLF4J logger `checkpoint.metrics`:

```
{"event":"checkpoint_created","version":7,"createdAtMs":...,"totalMs":...,
 "buildMs":...,"diffMs":...,"uploadMs":...,"sstNew":4,"sstReused":56,
 "bytesNew":...,"bytesTotal":...,"offsets":{...}}

{"event":"checkpoint_restored","version":7,"totalMs":...,"sstCount":60,
 "metaCount":4,"bytesTotal":...,"offsets":{...}}
```

These are the lines to grep/parse for benchmark analysis. Same field shape
across local FS, MinIO, and real S3 backends — plotting across backends is
just a `cat | jq` pipeline.

If you add new phases or new tradeoffs, add corresponding fields to these
events. Don't make new events unnecessarily; benchmarks need stable shape.
