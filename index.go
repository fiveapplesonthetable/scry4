package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"kythe.io/kythe/go/platform/kzip"
	"kythe.io/kythe/go/services/graphstore"
	"kythe.io/kythe/go/serving/pipeline"
	"kythe.io/kythe/go/storage/leveldb"
	"kythe.io/kythe/go/storage/stream"
	"kythe.io/kythe/go/util/kytheuri"
	"kythe.io/kythe/go/util/markedsource"

	apb "kythe.io/kythe/proto/analysis_go_proto"
	cpb "kythe.io/kythe/proto/common_go_proto"
	spb "kythe.io/kythe/proto/storage_go_proto"
)

// indexStream is scry4's standalone, in-process kzip → serving pipeline.
// It depends only on Kythe (not scry3): Kythe's kzip reader yields CUs, the
// matching indexer binary is run per CU, and the resulting Entry stream is
// folded straight into a LevelDB GraphStore in-process (no `write_entries`
// subprocess) while names are extracted in the same pass. At the end the
// serving table is built in-process via the Kythe serving pipeline (no
// `write_tables` subprocess). Resumable via <graphstore>.done + .names.
type indexArgs struct {
	kzipPath   string
	kytheRoot  string
	out        string // serving table dir
	names      string // name index path ("" = skip)
	graphstore string
	langs      string
	in, notIn  []string
	jvmHeap    string
	inject     []injectRule
	workers    int
	resume     bool
	keepGS     bool
}

// runIndexStream parses `scry4 <out-serving> index-stream [flags]`.
func runIndexStream(out string, toks []string) {
	a := indexArgs{out: out, langs: "cxx,java,jvm,go,proto,textproto", jvmHeap: "8g"}
	a.kytheRoot = os.Getenv("KYTHE_ROOT")
	var inject []string
	nextv := func(i *int) string {
		*i++
		if *i < len(toks) {
			return toks[*i]
		}
		return ""
	}
	csv := func(s string) []string {
		var o []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				o = append(o, p)
			}
		}
		return o
	}
	for i := 0; i < len(toks); i++ {
		switch toks[i] {
		case "--kzip":
			a.kzipPath = nextv(&i)
		case "--kythe-root":
			a.kytheRoot = nextv(&i)
		case "--graphstore":
			a.graphstore = nextv(&i)
		case "--names":
			a.names = nextv(&i)
		case "--langs":
			a.langs = nextv(&i)
		case "--jvm-heap":
			a.jvmHeap = nextv(&i)
		case "--in":
			a.in = csv(nextv(&i))
		case "--not-in":
			a.notIn = csv(nextv(&i))
		case "--workers":
			fmt.Sscanf(nextv(&i), "%d", &a.workers)
		case "--inject-cu-arg":
			inject = append(inject, nextv(&i))
		case "--resume":
			a.resume = true
		case "--keep-graphstore":
			a.keepGS = true
		}
	}
	if a.kzipPath == "" {
		die("index-stream needs --kzip")
	}
	if a.kytheRoot == "" {
		die("index-stream needs --kythe-root or $KYTHE_ROOT")
	}
	if a.graphstore == "" {
		a.graphstore = out + ".graphstore"
	}
	if a.names == "" {
		a.names = out + "/scry3.names.idx"
	}
	a.inject = parseInject(inject)
	indexStream(a)
}

type injectRule struct{ prefix, arg string }

func parseInject(raw []string) []injectRule {
	var out []injectRule
	for _, r := range raw {
		if i := strings.Index(r, "::"); i > 0 {
			out = append(out, injectRule{r[:i], r[i+2:]})
		}
	}
	return out
}

// routeLang maps a CU language to its indexer binary (relative to kythe-root).
func indexerCmd(kytheRoot, lang, subKzip, jvmHeap, jvmTmp string) (*exec.Cmd, bool) {
	j := func(p string) string { return filepath.Join(kytheRoot, p) }
	switch lang {
	case "c++":
		return exec.Command(j("indexers/cxx_indexer"), subKzip), true
	case "go":
		return exec.Command(j("indexers/go_indexer"), subKzip), true
	case "java", "jvm":
		jar := "indexers/java_indexer.jar"
		if lang == "jvm" {
			jar = "indexers/jvm_indexer.jar"
		}
		return exec.Command("java", "-Xmx"+jvmHeap, "-jar", j(jar),
			"--ignore_empty_kzip", "--temp_directory", jvmTmp, subKzip), true
	case "protobuf", "proto":
		return exec.Command(j("indexers/proto_indexer"), "-index_file="+subKzip), true
	case "textproto":
		return exec.Command(j("indexers/textproto_indexer"), "--index_file="+subKzip), true
	}
	return nil, false
}

func primaryPath(cu *apb.CompilationUnit) string {
	if s := cu.GetSourceFile(); len(s) > 0 {
		return s[0]
	}
	if r := cu.GetRequiredInput(); len(r) > 0 {
		return r[0].GetInfo().GetPath()
	}
	return ""
}

// writeSubKzip writes a single-CU kzip (mutated args) with all its file blobs.
func writeSubKzip(rd *kzip.Reader, cu *apb.CompilationUnit, idx *apb.IndexedCompilation_Index, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	w, err := kzip.NewWriter(f)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, fi := range cu.GetRequiredInput() {
		d := fi.GetInfo().GetDigest()
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		bits, err := rd.ReadAll(d)
		if err != nil {
			continue // compiler builtin / external; indexer tolerates
		}
		if _, err := w.AddFile(strings.NewReader(string(bits))); err != nil {
			return err
		}
	}
	if _, err := w.AddUnit(cu, idx); err != nil {
		return err
	}
	return w.Close()
}

type idxStats struct {
	mu                sync.Mutex
	ok, empty, failed int
	tails             []string
}

func (s *idxStats) fail(msg string) {
	s.mu.Lock()
	s.failed++
	if len(s.tails) < 8 {
		s.tails = append(s.tails, msg)
	}
	s.mu.Unlock()
}

func indexStream(a indexArgs) {
	ctx := context.Background()
	t0 := time.Now()
	if _, err := os.Stat(a.out); err == nil {
		die("serving table %q already exists; remove it first", a.out)
	}
	donePath := a.graphstore + ".done"
	namesDur := a.graphstore + ".names"

	// Resume: load done shas; require --resume to reuse an existing GraphStore.
	done := map[string]bool{}
	if _, err := os.Stat(a.graphstore); err == nil {
		if !a.resume {
			die("graphstore %q exists; pass --resume to continue, or remove it", a.graphstore)
		}
		if b, err := os.ReadFile(donePath); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if l = strings.TrimSpace(l); l != "" {
					done[l] = true
				}
			}
		}
		fmt.Fprintf(os.Stderr, "scry4: --resume: %d CUs already folded\n", len(done))
	}

	// Open source kzip.
	kf, err := os.Open(a.kzipPath)
	if err != nil {
		die("open kzip: %v", err)
	}
	defer kf.Close()
	fi, _ := kf.Stat()
	rd, err := kzip.NewReader(kf, fi.Size())
	if err != nil {
		die("kzip reader: %v", err)
	}

	// Plan: collect matching CUs.
	want := map[string]bool{}
	for _, l := range strings.Split(a.langs, ",") {
		want[strings.TrimSpace(l)] = true
	}
	langKey := func(l string) string {
		switch l {
		case "c++":
			return "cxx"
		case "protobuf":
			return "proto"
		}
		return l
	}
	accept := func(p string) bool {
		if len(a.in) > 0 {
			ok := false
			for _, s := range a.in {
				if s != "" && strings.Contains(p, s) {
					ok = true
				}
			}
			if !ok {
				return false
			}
		}
		for _, s := range a.notIn {
			if s != "" && strings.Contains(p, s) {
				return false
			}
		}
		return true
	}
	type cu struct {
		proto  *apb.CompilationUnit
		index  *apb.IndexedCompilation_Index
		digest string
		lang   string
	}
	var plan []cu
	fmt.Fprintf(os.Stderr, "scry4: scanning %s …\n", a.kzipPath)
	err = rd.Scan(func(u *kzip.Unit) error {
		lang := u.Proto.GetVName().GetLanguage()
		if !want[langKey(lang)] {
			return nil
		}
		if !accept(primaryPath(u.Proto)) {
			return nil
		}
		if done[u.Digest] {
			return nil
		}
		if _, ok := indexerCmd(a.kytheRoot, lang, "x", a.jvmHeap, "x"); !ok {
			return nil
		}
		plan = append(plan, cu{u.Proto, u.Index, u.Digest, lang})
		return nil
	})
	if err != nil {
		die("scan: %v", err)
	}
	fmt.Fprintf(os.Stderr, "scry4: plan: %d CUs\n", len(plan))

	// Staging temp lives under /mnt/agent/tmp (the big volume) — never /tmp.
	tmpBase := os.Getenv("SCRY_TMP_DIR")
	if tmpBase == "" {
		tmpBase = "/mnt/agent/tmp"
	}
	_ = os.MkdirAll(tmpBase, 0755)
	staging, _ := os.MkdirTemp(tmpBase, "scry4-index-")
	defer os.RemoveAll(staging)

	// In-process GraphStore (creates if absent; reused on resume).
	gs, err := leveldb.OpenGraphStore(a.graphstore, nil)
	if err != nil {
		die("open graphstore: %v", err)
	}

	// Durable name sink + done log.
	wantNames := a.names != ""
	var namesMu sync.Mutex
	nameSet := map[[2]string]bool{}
	var nameFile *bufio.Writer
	if wantNames {
		if b, err := os.ReadFile(namesDur); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if i := strings.IndexByte(l, '\t'); i > 0 {
					nameSet[[2]string{l[:i], l[i+1:]}] = true
				}
			}
			if len(nameSet) > 0 {
				fmt.Fprintf(os.Stderr, "scry4: --resume: preloaded %d name rows\n", len(nameSet))
			}
		}
		nf, err := os.OpenFile(namesDur, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			die("open names: %v", err)
		}
		defer nf.Close()
		nameFile = bufio.NewWriter(nf)
		defer nameFile.Flush()
	}
	addName := func(name, ticket string) {
		k := [2]string{name, ticket}
		namesMu.Lock()
		if !nameSet[k] {
			nameSet[k] = true
			fmt.Fprintf(nameFile, "%s\t%s\n", name, ticket)
		}
		namesMu.Unlock()
	}
	doneF, _ := os.OpenFile(donePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	defer doneF.Close()
	var doneMu sync.Mutex
	logDone := func(d string) {
		doneMu.Lock()
		fmt.Fprintln(doneF, d)
		doneMu.Unlock()
	}

	stats := &idxStats{}
	var idxDone int64
	workers := a.workers
	if workers == 0 {
		// Match scry2's default: half the cores. The memory ceiling is
		// (concurrent JVM indexers × --jvm-heap) + (cxx_indexer RSS × workers);
		// raise --workers for throughput, lower it (or --jvm-heap) to avoid OOM.
		workers = runtime.NumCPU() / 2
		if workers < 1 {
			workers = 1
		}
	}
	// Decouple indexing (parallel) from folding (serial, single LevelDB
	// writer): workers run indexers and write each CU's entries to a temp
	// file, then hand the path to a single folder goroutine via a bounded
	// channel. Folding never blocks the indexers, and the bounded channel
	// caps how many entry files sit on disk at once.
	type job struct{ digest, path string }
	ch := make(chan job, workers*2)

	var folderWg sync.WaitGroup
	folderWg.Add(1)
	go func() {
		defer folderWg.Done()
		for j := range ch {
			ef, err := os.Open(j.path)
			if err != nil {
				os.Remove(j.path)
				continue
			}
			reqs := graphstore.BatchWrites(stream.ReadEntries(ef), 1024)
			for req := range reqs {
				_ = gs.Write(ctx, req) // single writer — no mutex needed
				if !wantNames {
					continue
				}
				ticket := kytheuri.ToString(req.GetSource())
				for _, u := range req.GetUpdate() {
					if u.GetEdgeKind() == "/kythe/edge/named" && u.GetTarget().GetSignature() != "" {
						sig := u.GetTarget().GetSignature()
						addName(sig, ticket)
						if p, ok := stripJVMDesc(sig); ok {
							addName(p, ticket)
						}
					} else if u.GetFactName() == "/kythe/code" && len(u.GetFactValue()) > 0 {
						var ms cpb.MarkedSource
						if proto.Unmarshal(u.GetFactValue(), &ms) == nil {
							if si := markedsource.RenderQualifiedName(&ms); si.GetQualifiedName() != "" {
								addName(si.GetQualifiedName(), ticket)
							}
						}
					}
				}
			}
			ef.Close()
			os.Remove(j.path)
			stats.mu.Lock()
			stats.ok++
			stats.mu.Unlock()
			logDone(j.digest)
			n := atomic.AddInt64(&idxDone, 1)
			if n%200 == 0 {
				fmt.Fprintf(os.Stderr, "scry4: folded %d/%d (graphstore %s)\n", n, len(plan),
					func() string { fi, _ := os.Stat(a.graphstore); _ = fi; return "" }())
			}
		}
	}()

	var next int64 = -1
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&next, 1)
				if int(i) >= len(plan) {
					return
				}
				c := plan[int(i)]
				sub := filepath.Join(staging, c.digest+".kzip")
				ent := filepath.Join(staging, c.digest+".entries")
				jvmTmp := filepath.Join(staging, c.digest+".jvmtmp")
				if c.lang == "java" || c.lang == "jvm" {
					_ = os.MkdirAll(jvmTmp, 0755)
				}
				// Mutate a copy: strip AFDO, apply inject rules.
				cuc := proto.Clone(c.proto).(*apb.CompilationUnit)
				pp := primaryPath(cuc)
				var args []string
				for _, ar := range cuc.GetArgument() {
					if !strings.HasPrefix(ar, "-fprofile-sample-use") {
						args = append(args, ar)
					}
				}
				for _, r := range a.inject {
					if strings.HasPrefix(pp, r.prefix) {
						has := false
						for _, e := range args {
							if e == r.arg {
								has = true
							}
						}
						if !has {
							args = append([]string{r.arg}, args...)
						}
					}
				}
				cuc.Argument = args
				if err := writeSubKzip(rd, cuc, c.index, sub); err != nil {
					stats.fail(fmt.Sprintf("digest=%s subkzip: %v", c.digest, err))
					os.Remove(sub)
					continue
				}
				cmd, _ := indexerCmd(a.kytheRoot, c.lang, sub, a.jvmHeap, jvmTmp)
				outF, ferr := os.Create(ent)
				if ferr != nil {
					stats.fail(fmt.Sprintf("digest=%s create: %v", c.digest, ferr))
					os.Remove(sub)
					continue
				}
				cmd.Stdout = outF // indexer stdout → file (no fold contention)
				var stderr strings.Builder
				cmd.Stderr = &stderr
				werr := cmd.Run()
				outF.Close()
				os.Remove(sub)
				os.RemoveAll(jvmTmp)
				if werr != nil {
					tail := stderr.String()
					if len(tail) > 200 {
						tail = tail[len(tail)-200:]
					}
					stats.fail(fmt.Sprintf("digest=%s exit: %v %s", c.digest, werr, strings.ReplaceAll(tail, "\n", " ")))
					os.Remove(ent)
					continue
				}
				if fi, _ := os.Stat(ent); fi == nil || fi.Size() == 0 {
					stats.mu.Lock()
					stats.empty++
					stats.mu.Unlock()
					os.Remove(ent)
					continue
				}
				ch <- job{c.digest, ent} // blocks if folder behind (backpressure)
			}
		}()
	}
	wg.Wait()
	close(ch)
	folderWg.Wait()
	if wantNames {
		nameFile.Flush()
	}
	_ = gs.Close(ctx)
	fmt.Fprintf(os.Stderr, "scry4: indexed ok=%d empty=%d failed=%d\n", stats.ok, stats.empty, stats.failed)
	for _, t := range stats.tails {
		fmt.Fprintf(os.Stderr, "scry4:   ! %s\n", t)
	}

	// Build serving table in-process from the GraphStore.
	fmt.Fprintf(os.Stderr, "scry4: building serving table %s …\n", a.out)
	gs2, err := leveldb.OpenGraphStore(a.graphstore, &leveldb.Options{MustExist: true})
	if err != nil {
		die("reopen graphstore: %v", err)
	}
	_ = os.MkdirAll(a.out, 0755) // leveldb creates the db dir's contents, not its parents
	db, err := leveldb.Open(a.out, nil)
	if err != nil {
		die("create serving: %v", err)
	}
	rd2 := func(f func(*spb.Entry) error) error {
		return gs2.Scan(ctx, &spb.ScanRequest{}, f)
	}
	if err := pipeline.Run(ctx, rd2, db, &pipeline.Options{MaxPageSize: 4000, MaxShardSize: 32000}); err != nil {
		die("pipeline: %v", err)
	}
	_ = db.Close(ctx)
	_ = gs2.Close(ctx)

	// Name index.
	if wantNames {
		rows := make([][2]string, 0, len(nameSet))
		for k := range nameSet {
			rows = append(rows, k)
		}
		sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })
		out := a.names
		w, err := os.Create(out)
		if err != nil {
			die("create name index: %v", err)
		}
		bw := bufio.NewWriter(w)
		for _, r := range rows {
			fmt.Fprintf(bw, "%s\t%s\n", r[0], r[1])
		}
		bw.Flush()
		w.Close()
		fmt.Fprintf(os.Stderr, "scry4: name index: %d rows → %s\n", len(rows), out)
	}

	if !a.keepGS {
		os.RemoveAll(a.graphstore)
		os.Remove(donePath)
		os.Remove(namesDur)
	}
	fmt.Fprintf(os.Stderr, "scry4: done in %.1f min → %s\n", time.Since(t0).Minutes(), a.out)
}

func less(a, b [2]string) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	return a[1] < b[1]
}
