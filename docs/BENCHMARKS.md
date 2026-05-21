# scry2 vs scry3 vs scry3-streaming vs scry4

All four consume the **same** Kythe entries (the indexing stage — Clang/javac
compiling each TU — is identical and shared). They differ only in how the
data is stored and queried. Benchmark input: one C++ CU,
`frameworks/native/libs/binder/Parcel.cpp` + 1015 headers = 3.06 M entries.
Host: Xeon Gold 6148, 72 vCPU, 157 GB RAM, SSD. Warm numbers are medians of
5–500 runs.

## The four designs

| | storage | query transport |
|---|---|---|
| **scry2** | custom packed `.s2db`, mmap | in-process `memcmp` binary search |
| **scry3** | stock Kythe LevelDB serving table | spawns the `kythe` CLI per query |
| **scry3 `--http`** | same serving table | one warm `http_server`, kept-alive socket from scry3 |
| **scry4** | same serving table | Kythe Go serving libs, **in-process** |

## Query latency

| path | scry2 | scry3 CLI | scry3 `--http` | scry4 |
|---|---|---|---|---|
| **warm / amortized (repl)** | **44 µs** | — | 1.75 ms | **179 µs** |
| one-shot (cold process) | 40 ms | 170 ms | ~70 ms | ~110 ms |
| server-side query alone | ~1.8 µs | 537 µs | 537 µs | ~179 µs (no transport) |

**Why these land where they do.** A warm `kythe` serving query is ~500 µs;
the rest is overhead. `strace` of the `kythe` CLI shows ~89 % of a one-shot
is **Go runtime startup** (futex), not LevelDB. So:

* **scry3 CLI** pays that ~40 ms Go startup *every* query → 170 ms one-shot,
  no warm mode.
* **scry3 `--http`** removes per-query startup (one warm server) but pays a
  localhost HTTP round-trip + JSON encode/parse → 1.75 ms.
* **scry4** removes *all* of it: same process, no socket, no JSON — a direct
  call into `xrefs.Service` → **179 µs**, essentially the serving query
  itself.
* **scry2** is faster still (44 µs) because its `.s2db` is a leaner schema
  (offsets, no snippets/decorations) read by a `memcmp` — but that's a custom
  engine returning less data.

scry4 closes ~90 % of the gap from scry3 to scry2 **without** a custom engine
and **without** a warm server process.

## Data returned per hit

* **scry2** — byte offsets (`Parcel.cpp@58156`), curated edge subset.
* **scry3 / scry4** — source snippet, full line/column span, every edge kind,
  node facts, decorations — straight from Kythe.

scry4's `def` even resolves the real `.cpp` definition body (via Kythe's
canonical MarkedSource names), e.g.:

```
def android::Parcel::writeStrongBinder
  frameworks/native/libs/binder/Parcel.cpp:1677:0  status_t Parcel::writeStrongBinder(const sp<IBinder>& val)
```

## Name index quality

Both build a `name<TAB>ticket` index from the same entries. scry4 uses
Kythe's own `markedsource.RenderQualifiedName`; scry3 hand-rolls a MarkedSource
parser.

| | rows (same input) | notes |
|---|---|---|
| scry3 | 83,847 | includes noisy/duplicated renderings (`Parcel::android::Parcel::…`) |
| scry4 | **45,727** | canonical FQNs only — fewer, cleaner, no garbled context prefixes |

## Build, RAM, disk (the scaling story)

The indexing stage (kzip → entries) is identical and shared. The divergence
is storage:

| | build (entries → queryable) | peak RAM | disk model |
|---|---|---|---|
| **scry2** | 8 s, 47 MB `.s2db` | **~103 GB** on a 23.6k-CU AOSP slice (streams entries into RAM) | low disk, very high RAM |
| **scry3** `index`+`build` | ~133 s, 459 MB serving | low | **multi-TB entries on disk** at AOSP scale (does not fit) |
| **scry3 `index-stream`** | one pass | **2.0 GB** | bounded: GraphStore (~tens of GB) + serving; entries never accumulate |
| **scry4 `build`** | in-process `pipeline.Run` from a GraphStore | like `write_tables` | consumes the streamed GraphStore |

`scry3 index-stream` is the AOSP-viable builder (bounded disk *and* RAM); feed
its `--keep-graphstore` output to `scry4 build` to construct the serving
table in-process, then query with scry4.

## Bottom line

* Want **µs latency + tiny index**, offsets are enough → **scry2**.
* Want **zero build, stock tools**, ~2 ms is fine → **scry3 (`--http`)**.
* Want **near-µs latency + full Kythe data + no custom engine** and can build
  against Kythe's Go source → **scry4**. Best overall balance.
* To **build at AOSP scale on a normal disk** → **scry3 `index-stream`**
  (then optionally `scry4 build` + query).

## Standalone

scry4 does **not** depend on scry3. Its own `index-stream` builds the serving
table from a kzip fully in-process — Kythe's Go kzip reader yields CUs, the
indexer binaries run per CU, entries fold straight into a LevelDB GraphStore
in-process (no `write_entries`), and the serving table is built via the Kythe
serving pipeline in-process (no `write_tables`). Bounded disk + RAM, resumable
with `--resume`. So the whole pipeline is one Go binary + the Kythe indexer
binaries.

## Reproduce

```bash
export KYTHE_ROOT=/path/to/kythe-v0.0.75   # patched indexers
# build serving + name index from a kzip, in-process (bounded disk/RAM, resumable):
scry4 OUT.serving index-stream --kzip K --in <dirs> \
      --langs cxx,java,jvm --jvm-heap 12g --workers 24 [--resume]
# query in-process:
scry4 OUT.serving repl     # or: scry4 OUT.serving def NAME / ref NAME / callgraph NAME
```
