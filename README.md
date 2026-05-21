# scry4

A deep-integration, **in-process** Kythe code-walker in Go — the sibling of
[scry2](https://github.com/fiveapplesonthetable/scry2) (custom `.s2db` engine)
and [scry3](https://github.com/fiveapplesonthetable/scry3) (stock Kythe via
the `kythe` CLI / a warm `http_server`).

scry3 proved the only thing slow about stock Kythe is **per-query process
startup** — the warm serving query itself is ~500 µs. scry4 removes that
startup entirely: it **links Kythe's Go serving libraries directly** and
answers every query in-process against the LevelDB serving table. No
subprocess, no HTTP, no JSON. Result: **179 µs/query warm** — ~10× faster
than scry3's warm path and within ~4× of scry2's hand-rolled mmap — while
keeping *full* Kythe data and **zero custom storage/query engine**.

```
scry2:  custom .s2db + mmap        →  44 µs warm,  offsets only,  own engine
scry3:  stock kythe CLI / http     →  1.75 ms warm, full data,    no engine, needs warm server
scry4:  Kythe Go libs, in-process  →  179 µs warm, full data,     no engine, no server
```

## How it works

scry4 imports Kythe directly:

* `kythe.io/kythe/go/storage/leveldb` — opens the serving table (once).
* `kythe.io/kythe/go/serving/{xrefs,graph}` — `xrefs.NewService` /
  `graph.NewService` answer `CrossReferences` / `Edges` / `Nodes`
  **in-process**.
* `kythe.io/kythe/go/util/kytheuri` — ticket parse/format (no hand-rolling).
* `kythe.io/kythe/go/util/markedsource` — `RenderQualifiedName` builds the
  name→ticket index from `/kythe/code` (cleaner than scry3's hand-rolled
  MarkedSource parser: 45,727 canonical names vs scry3's 83,847 noisier ones
  on the same input).
* `kythe.io/kythe/go/serving/pipeline` — `build` turns a GraphStore into a
  serving table **in-process** (no `write_tables` subprocess).

The name index is the same `name<TAB>ticket` file scry3 writes, so the two
tools share index files.

## Usage

```bash
# query a serving table (built by scry3 index-stream, or scry4 build)
scry4 <serving> def      android::Parcel::writeStrongBinder
scry4 <serving> ref      android::Parcel::writeInt32
scry4 <serving> callers  android::Parcel::writeInt32
scry4 <serving> super NAME / sub NAME      [--in S] [--not-in S]
scry4 <serving> callgraph NAME --direction up|down|both --depth N
scry4 <serving> edges NAME / nodes NAME / identifier NAME
scry4 <serving> repl                       # warm in-process loop — the fast path
scry4 <serving> stat

# build helpers
scry4 <serving> name-index <entries-dir> [out]   # Go name index (markedsource)
scry4 <serving> build      <graphstore-dir>      # graphstore → serving, in-process
```

Name resolution uses `<serving>/scry3.names.idx` by default (`$SCRY4_NAMES`
to override). Indexing (kzip → entries/GraphStore) is shared with scry3 —
run `scry3 index-stream --keep-graphstore`, then `scry4 build` and query.

## Build

scry4 is a deep integration, so it builds **against a local Kythe source
checkout** and needs the native LevelDB lib (Kythe's `leveldb` is cgo):

```bash
# 1. Kythe v0.0.75 source next to this repo (../kythe-source), or edit the
#    `replace kythe.io => ../kythe-source` line in go.mod.
git clone --depth=1 -b v0.0.75 https://github.com/kythe/kythe.git ../kythe-source

# 2. native leveldb (cgo)
sudo apt-get install -y libleveldb-dev libsnappy-dev

# 3. build
CGO_ENABLED=1 go build -o scry4 .
```

## Benchmarks

Full four-way numbers (scry2 / scry3 / scry3-streaming / scry4) on the same
input are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md). Headline (one C++ CU,
warm query):

| | engine | warm query | data | query-time deps |
|---|---|---|---|---|
| scry2 | custom `.s2db` mmap | **44 µs** | offsets, curated | none (in-proc) |
| scry3 | stock kythe CLI | 170 ms one-shot | full | spawns `kythe` |
| scry3 `--http` | warm `http_server` | 1.75 ms | full | warm server |
| **scry4** | **Kythe Go, in-process** | **179 µs** | **full** | **none (in-proc)** |

## When to use which

* **scry2** — absolute minimum latency / very high QPS; lean offset data is enough.
* **scry3** — zero-build wrapper over stock tools; the warm-server path is fine at ~2 ms.
* **scry4** — want scry2-class latency *and* full Kythe data with no custom
  engine, and can build against Kythe's Go source. **The best balance for
  most code-walking.**

## Docs

* [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — four-way latency/build/RAM/disk comparison and analysis.
