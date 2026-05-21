// scry4 — a deep-integration, in-process Go port of scry3.
//
// scry3 wraps the stock `kythe` CLI / a warm `http_server` over a socket.
// scry4 links Kythe's serving libraries directly and answers queries
// IN-PROCESS against the LevelDB serving table — no subprocess, no HTTP, no
// JSON. The warm process opens the table once; each query is a direct call
// into kythe.io/kythe/go/serving/{xrefs,graph}. Name→ticket resolution uses
// Kythe's own markedsource renderer + kytheuri (not a hand-rolled parser).
//
// Verb/flag surface mirrors scry2: def | ref | callers | super | sub |
// callgraph | edges | nodes | identifier | stat | repl, with
// --substr / --in / --not-in / --limit / --direction / --depth.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"kythe.io/kythe/go/services/graph"
	"kythe.io/kythe/go/services/xrefs"
	gsrv "kythe.io/kythe/go/serving/graph"
	xsrv "kythe.io/kythe/go/serving/xrefs"
	"kythe.io/kythe/go/storage/leveldb"
	"kythe.io/kythe/go/util/kytheuri"

	gpb "kythe.io/kythe/proto/graph_go_proto"
	xpb "kythe.io/kythe/proto/xref_go_proto"
)

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "scry4: "+format+"\n", a...)
	os.Exit(1)
}

func usage() {
	fmt.Fprint(os.Stderr, `scry4 — in-process Kythe queries (Go)

USAGE:
  scry4 <serving> <verb> <name|ticket> [flags]      one-shot
  scry4 <serving> repl                               warm loop (fast)
  scry4 <serving> name-index <entries-dir> [out]     build name index (Go)
  scry4 <serving> build <graphstore-dir>             graphstore → serving (in-process)

VERBS:  def ref callers super sub callgraph edges nodes identifier stat
FLAGS:  --substr  --in S  --not-in S  --limit N  --direction up|down|both  --depth N
name resolution uses <serving>/scry3.names.idx (override with $SCRY4_NAMES).
`)
	os.Exit(2)
}

// qflags are the scry2-parity options shared by the query verbs.
type qflags struct {
	substr    bool
	in, notIn string
	limit     int
	direction string
	depth     int
}

func parseFlags(toks []string) (arg string, f qflags) {
	f.limit, f.direction, f.depth = 50, "up", 3
	for i := 0; i < len(toks); i++ {
		switch toks[i] {
		case "--substr":
			f.substr = true
		case "--in":
			i++
			if i < len(toks) {
				f.in = toks[i]
			}
		case "--not-in":
			i++
			if i < len(toks) {
				f.notIn = toks[i]
			}
		case "--limit":
			i++
			if i < len(toks) {
				f.limit, _ = strconv.Atoi(toks[i])
			}
		case "--direction":
			i++
			if i < len(toks) {
				f.direction = toks[i]
			}
		case "--depth":
			i++
			if i < len(toks) {
				f.depth, _ = strconv.Atoi(toks[i])
			}
		default:
			if arg == "" {
				arg = toks[i]
			}
		}
	}
	return
}

func (f qflags) keep(path string) bool {
	if f.in != "" && !strings.Contains(path, f.in) {
		return false
	}
	if f.notIn != "" && strings.Contains(path, f.notIn) {
		return false
	}
	return true
}

type engine struct {
	ctx   context.Context
	xs    xrefs.Service
	gs    graph.Service
	names *nameIndex
}

func openEngine(serving string) (*engine, func()) {
	ctx := context.Background()
	db, err := leveldb.Open(serving, &leveldb.Options{MustExist: true})
	if err != nil {
		die("open serving table %q: %v", serving, err)
	}
	e := &engine{ctx: ctx, xs: xsrv.NewService(ctx, db), gs: gsrv.NewService(ctx, db)}
	np := os.Getenv("SCRY4_NAMES")
	if np == "" {
		np = serving + "/scry3.names.idx"
	}
	if idx, err := loadNameIndex(np); err == nil {
		e.names = idx
	}
	return e, func() { _ = db.Close(ctx) }
}

func isTicket(s string) bool { return strings.HasPrefix(s, "kythe:") }

func (e *engine) resolve(name string, f qflags) []string {
	if isTicket(name) {
		return []string{name}
	}
	if e.names == nil {
		die("no name index (build one: scry4 <serving> name-index <entries-dir>)")
	}
	t := e.names.lookup(name, f.substr)
	if len(t) == 0 {
		die("no ticket for %q (try --substr)", name)
	}
	if len(t) > f.limit {
		t = t[:f.limit]
	}
	return t
}

func pathOf(ticket string) string {
	if u, err := kytheuri.Parse(ticket); err == nil && u.Path != "" {
		return u.Path
	}
	return ticket
}

func sigOf(ticket string) string {
	if u, err := kytheuri.Parse(ticket); err == nil {
		return u.Signature
	}
	return ticket
}

func printAnchors(ras []*xpb.CrossReferencesReply_RelatedAnchor, f qflags) int {
	n := 0
	for _, ra := range ras {
		a := ra.GetAnchor()
		if a == nil {
			continue
		}
		path := pathOf(a.GetParent())
		if path == "" {
			path = pathOf(a.GetTicket())
		}
		if !f.keep(path) {
			continue
		}
		start := a.GetSpan().GetStart()
		text := strings.TrimSpace(a.GetSnippet())
		if text == "" {
			text = strings.TrimSpace(a.GetText())
		}
		fmt.Printf("  %s:%d:%d  %s\n", path, start.GetLineNumber(), start.GetColumnOffset(), text)
		n++
		if n >= f.limit {
			break
		}
	}
	return n
}

func (e *engine) xref(verb, name string, f qflags,
	def xpb.CrossReferencesRequest_DefinitionKind,
	refk xpb.CrossReferencesRequest_ReferenceKind,
	callk xpb.CrossReferencesRequest_CallerKind) {
	for _, t := range e.resolve(name, f) {
		reply, err := e.xs.CrossReferences(e.ctx, &xpb.CrossReferencesRequest{
			Ticket:         []string{t},
			DefinitionKind: def,
			ReferenceKind:  refk,
			CallerKind:     callk,
			Snippets:       xpb.SnippetsKind_DEFAULT,
		})
		if err != nil {
			die("xrefs: %v", err)
		}
		total := 0
		for _, set := range reply.GetCrossReferences() {
			fmt.Printf("%s %s [%s]\n", verb, name, sigOf(t))
			total += printAnchors(set.GetDefinition(), f)
			total += printAnchors(set.GetReference(), f)
			total += printAnchors(set.GetCaller(), f)
		}
		if total == 0 {
			fmt.Printf("%s %s [%s]\n  (none)\n", verb, name, sigOf(t))
		}
	}
}

var inheritKinds = []string{
	"/kythe/edge/extends", "/kythe/edge/extends/public",
	"/kythe/edge/extends/protected", "/kythe/edge/extends/private",
	"/kythe/edge/overrides", "/kythe/edge/satisfies",
}

func (e *engine) inheritKindList(sub bool) []string {
	kinds := make([]string, len(inheritKinds))
	for i, k := range inheritKinds {
		if sub {
			kinds[i] = "%" + k
		} else {
			kinds[i] = k
		}
	}
	return kinds
}

// edgeTargets returns the deduped target tickets of `ticket` over `kinds`.
func (e *engine) edgeTargets(ticket string, kinds []string) []string {
	reply, err := e.gs.Edges(e.ctx, &gpb.EdgesRequest{Ticket: []string{ticket}, Kind: kinds})
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, es := range reply.GetEdgeSets() {
		for _, grp := range es.GetGroups() {
			for _, ed := range grp.GetEdge() {
				tt := ed.GetTargetTicket()
				if tt != "" && !seen[tt] {
					seen[tt] = true
					out = append(out, tt)
				}
			}
		}
	}
	return out
}

func (e *engine) inheritance(name string, f qflags, sub bool) {
	kinds := e.inheritKindList(sub)
	verb := "super"
	if sub {
		verb = "sub"
	}
	for _, t := range e.resolve(name, f) {
		n := 0
		for _, tt := range e.edgeTargets(t, kinds) {
			if !f.keep(pathOf(tt)) {
				continue
			}
			if n == 0 {
				fmt.Printf("%s %s [%s]\n", verb, name, sigOf(t))
			}
			fmt.Printf("  %s  [%s]\n", sigOf(tt), pathOf(tt))
			n++
			if n >= f.limit {
				break
			}
		}
		if n == 0 {
			fmt.Printf("%s %s [%s]\n  (none)\n", verb, name, sigOf(t))
		}
	}
}

// callersOf — semantic callers (the functions whose bodies call `ticket`).
func (e *engine) callersOf(ticket string) []string {
	reply, err := e.xs.CrossReferences(e.ctx, &xpb.CrossReferencesRequest{
		Ticket:     []string{ticket},
		CallerKind: xpb.CrossReferencesRequest_DIRECT_CALLERS,
	})
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, set := range reply.GetCrossReferences() {
		for _, ra := range set.GetCaller() {
			if tk := ra.GetTicket(); tk != "" && !seen[tk] {
				seen[tk] = true
				out = append(out, tk)
			}
		}
	}
	return out
}

// calleesOf — functions called from `ticket`'s definition body: decorate the
// def file within the def span and collect /kythe/edge/ref/call targets.
func (e *engine) calleesOf(ticket string) []string {
	def, err := e.xs.CrossReferences(e.ctx, &xpb.CrossReferencesRequest{
		Ticket:         []string{ticket},
		DefinitionKind: xpb.CrossReferencesRequest_ALL_DEFINITIONS,
	})
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, set := range def.GetCrossReferences() {
		for _, ra := range set.GetDefinition() {
			a := ra.GetAnchor()
			file := a.GetParent()
			if file == "" || a.GetSpan() == nil {
				continue
			}
			dec, err := e.xs.Decorations(e.ctx, &xpb.DecorationsRequest{
				Location:   &xpb.Location{Ticket: file, Kind: xpb.Location_SPAN, Span: a.GetSpan()},
				SpanKind:   xpb.DecorationsRequest_WITHIN_SPAN,
				References: true,
			})
			if err != nil {
				continue
			}
			for _, ref := range dec.GetReference() {
				if !strings.Contains(ref.GetKind(), "ref/call") {
					continue
				}
				if tt := ref.GetTargetTicket(); tt != "" && !seen[tt] {
					seen[tt] = true
					out = append(out, tt)
				}
			}
		}
	}
	return out
}

func (e *engine) next(ticket, dir string) []string {
	switch dir {
	case "up":
		return e.callersOf(ticket)
	case "down":
		return e.calleesOf(ticket)
	default: // both
		return append(e.callersOf(ticket), e.calleesOf(ticket)...)
	}
}

func (e *engine) callgraph(name string, f qflags) {
	if f.direction != "up" && f.direction != "down" && f.direction != "both" {
		die("--direction must be up|down|both")
	}
	visited := map[string]bool{}
	var walk func(ticket string, depth int)
	walk = func(ticket string, depth int) {
		if depth > f.depth || visited[ticket] {
			return
		}
		visited[ticket] = true
		seen := map[string]bool{}
		for _, nx := range e.next(ticket, f.direction) {
			if seen[nx] || !f.keep(pathOf(nx)) {
				continue
			}
			seen[nx] = true
			fmt.Printf("%s%s  [%s]\n", strings.Repeat("  ", depth), sigOf(nx), pathOf(nx))
			walk(nx, depth+1)
		}
	}
	for _, t := range e.resolve(name, f) {
		fmt.Printf("callgraph %s (%s, depth %d) [%s]\n", name, f.direction, f.depth, sigOf(t))
		walk(t, 1)
	}
}

func (e *engine) edges(name string, f qflags) {
	for _, t := range e.resolve(name, f) {
		reply, err := e.gs.Edges(e.ctx, &gpb.EdgesRequest{Ticket: []string{t}})
		if err != nil {
			die("edges: %v", err)
		}
		for _, es := range reply.GetEdgeSets() {
			for kind, grp := range es.GetGroups() {
				for _, ed := range grp.GetEdge() {
					fmt.Printf("  %-32s %s\n", kind, ed.GetTargetTicket())
				}
			}
		}
	}
}

func (e *engine) nodes(name string, f qflags) {
	for _, t := range e.resolve(name, f) {
		reply, err := e.gs.Nodes(e.ctx, &gpb.NodesRequest{Ticket: []string{t}})
		if err != nil {
			die("nodes: %v", err)
		}
		for tk, ni := range reply.GetNodes() {
			fmt.Printf("%s\n", tk)
			facts := make([]string, 0, len(ni.GetFacts()))
			for fn := range ni.GetFacts() {
				facts = append(facts, fn)
			}
			sort.Strings(facts)
			for _, fn := range facts {
				fmt.Printf("  %-24s %s\n", fn, string(ni.GetFacts()[fn]))
			}
		}
	}
}

func (e *engine) identifier(name string, f qflags) {
	if e.names == nil {
		die("no name index")
	}
	if f.substr {
		for _, p := range e.names.substr(name) {
			fmt.Printf("%s\t%s\n", p.name, p.ticket)
		}
	} else {
		for _, t := range e.names.lookup(name, false) {
			fmt.Println(t)
		}
	}
}

func (e *engine) dispatch(verb, arg string, f qflags) {
	R := xpb.CrossReferencesRequest_NO_REFERENCES
	D := xpb.CrossReferencesRequest_NO_DEFINITIONS
	C := xpb.CrossReferencesRequest_NO_CALLERS
	switch verb {
	case "def":
		e.xref("def", arg, f, xpb.CrossReferencesRequest_ALL_DEFINITIONS, R, C)
	case "ref":
		e.xref("ref", arg, f, D, xpb.CrossReferencesRequest_ALL_REFERENCES, C)
	case "callers":
		e.xref("callers", arg, f, D, xpb.CrossReferencesRequest_CALL_REFERENCES, xpb.CrossReferencesRequest_DIRECT_CALLERS)
	case "super":
		e.inheritance(arg, f, false)
	case "sub":
		e.inheritance(arg, f, true)
	case "callgraph":
		e.callgraph(arg, f)
	case "edges":
		e.edges(arg, f)
	case "nodes":
		e.nodes(arg, f)
	case "identifier", "names":
		if verb == "names" {
			f.substr = true
		}
		e.identifier(arg, f)
	default:
		die("unknown verb %q", verb)
	}
}

func (e *engine) repl() {
	fmt.Fprintln(os.Stderr, "[repl] ready (in-process; def ref callers super sub callgraph edges nodes identifier; ^D to exit)")
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		toks := strings.Fields(line)
		verb := toks[0]
		arg, f := parseFlags(toks[1:])
		if arg == "" && verb != "stat" {
			fmt.Fprintln(os.Stderr, "[repl] usage: <verb> <name> [--substr --in S --not-in S --direction up|down|both --depth N]")
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[repl] error: %v\n", r)
				}
			}()
			e.dispatch(verb, arg, f)
		}()
	}
}

func (e *engine) stat(serving string) {
	fmt.Printf("serving table: %s\n", serving)
	if e.names != nil {
		fmt.Printf("  name index : %d rows\n", len(e.names.rows))
	} else {
		fmt.Printf("  name index : (none)\n")
	}
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}
	serving := os.Args[1]
	verb := os.Args[2]
	rest := os.Args[3:]

	switch verb {
	case "name-index":
		if len(rest) < 1 {
			die("name-index needs <entries-dir> [out]")
		}
		out := serving + "/scry3.names.idx"
		if len(rest) >= 2 {
			out = rest[1]
		}
		buildNameIndex(rest[0], out)
		return
	case "build":
		if len(rest) < 1 {
			die("build needs <graphstore-dir>")
		}
		buildServing(rest[0], serving)
		return
	}

	e, closeFn := openEngine(serving)
	defer closeFn()

	switch verb {
	case "repl":
		e.repl()
	case "stat":
		e.stat(serving)
	default:
		arg, f := parseFlags(rest)
		if arg == "" {
			die("%s needs <name|ticket>", verb)
		}
		e.dispatch(verb, arg, f)
	}
}
