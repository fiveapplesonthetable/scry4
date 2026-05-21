// scry4 — a deep-integration, in-process Go port of scry3.
//
// scry3 wraps the stock `kythe` CLI / a warm `http_server` over a socket.
// scry4 links Kythe's serving libraries directly and answers queries
// IN-PROCESS against the LevelDB serving table — no subprocess, no HTTP, no
// JSON. The warm process opens the table once; each query is a direct call
// into kythe.io/kythe/go/serving/{xrefs,graph}. Name→ticket resolution uses
// Kythe's own markedsource renderer + kytheuri (not a hand-rolled parser).
//
// Subcommands:
//   query:  def | ref | callers | super | sub | edges | nodes | identifier | stat | repl
//   build:  name-index (entries → names.idx) | build (graphstore → serving, in-process)
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
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
  scry4 <serving> <verb> <name|ticket> [--substr]      one-shot
  scry4 <serving> repl                                  warm loop (fast)
  scry4 <serving> name-index <entries-dir> [out]        build name index (Go)
  scry4 <serving> build <graphstore-dir>                graphstore → serving (in-process)

verbs: def ref callers super sub edges nodes identifier stat
name resolution uses <serving>/scry3.names.idx (override with $SCRY4_NAMES).
`)
	os.Exit(2)
}

// engine holds the in-process services + name index, opened once.
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

func (e *engine) resolve(name string, substr bool) []string {
	if isTicket(name) {
		return []string{name}
	}
	if e.names == nil {
		die("no name index (build one: scry4 <serving> name-index <entries-dir>)")
	}
	t := e.names.lookup(name, substr)
	if len(t) == 0 {
		die("no ticket for %q (try --substr)", name)
	}
	return t
}

// pathOf returns the file path of a ticket via Kythe's URI parser.
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

func printAnchors(ras []*xpb.CrossReferencesReply_RelatedAnchor) int {
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
		start := a.GetSpan().GetStart()
		text := strings.TrimSpace(a.GetSnippet())
		if text == "" {
			text = strings.TrimSpace(a.GetText())
		}
		fmt.Printf("  %s:%d:%d  %s\n", path, start.GetLineNumber(), start.GetColumnOffset(), text)
		n++
	}
	return n
}

func (e *engine) xref(verb, name string, substr bool,
	def xpb.CrossReferencesRequest_DefinitionKind,
	refk xpb.CrossReferencesRequest_ReferenceKind,
	callk xpb.CrossReferencesRequest_CallerKind) {
	for _, t := range e.resolve(name, substr) {
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
			total += printAnchors(set.GetDefinition())
			total += printAnchors(set.GetReference())
			total += printAnchors(set.GetCaller())
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

func (e *engine) inheritance(name string, substr, sub bool) {
	kinds := make([]string, len(inheritKinds))
	for i, k := range inheritKinds {
		if sub {
			kinds[i] = "%" + k
		} else {
			kinds[i] = k
		}
	}
	verb := "super"
	if sub {
		verb = "sub"
	}
	for _, t := range e.resolve(name, substr) {
		reply, err := e.gs.Edges(e.ctx, &gpb.EdgesRequest{Ticket: []string{t}, Kind: kinds})
		if err != nil {
			die("edges: %v", err)
		}
		n := 0
		for _, es := range reply.GetEdgeSets() {
			for _, grp := range es.GetGroups() {
				for _, ed := range grp.GetEdge() {
					if n == 0 {
						fmt.Printf("%s %s [%s]\n", verb, name, sigOf(t))
					}
					fmt.Printf("  %s  [%s]\n", sigOf(ed.GetTargetTicket()), pathOf(ed.GetTargetTicket()))
					n++
				}
			}
		}
		if n == 0 {
			fmt.Printf("%s %s [%s]\n  (none)\n", verb, name, sigOf(t))
		}
	}
}

func (e *engine) edges(name string) {
	for _, t := range e.resolve(name, false) {
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

func (e *engine) nodes(name string) {
	for _, t := range e.resolve(name, false) {
		reply, err := e.gs.Nodes(e.ctx, &gpb.NodesRequest{Ticket: []string{t}})
		if err != nil {
			die("nodes: %v", err)
		}
		for tk, ni := range reply.GetNodes() {
			fmt.Printf("%s\n", tk)
			facts := make([]string, 0, len(ni.GetFacts()))
			for f := range ni.GetFacts() {
				facts = append(facts, f)
			}
			sort.Strings(facts)
			for _, f := range facts {
				fmt.Printf("  %-24s %s\n", f, string(ni.GetFacts()[f]))
			}
		}
	}
}

func (e *engine) identifier(name string, substr bool) {
	if e.names == nil {
		die("no name index")
	}
	if substr {
		for _, p := range e.names.substr(name) {
			fmt.Printf("%s\t%s\n", p.name, p.ticket)
		}
	} else {
		for _, t := range e.names.lookup(name, false) {
			fmt.Println(t)
		}
	}
}

func (e *engine) dispatch(verb, arg string, substr bool) {
	R := xpb.CrossReferencesRequest_NO_REFERENCES
	D := xpb.CrossReferencesRequest_NO_DEFINITIONS
	C := xpb.CrossReferencesRequest_NO_CALLERS
	switch verb {
	case "def":
		e.xref("def", arg, substr, xpb.CrossReferencesRequest_ALL_DEFINITIONS, R, C)
	case "ref":
		e.xref("ref", arg, substr, D, xpb.CrossReferencesRequest_ALL_REFERENCES, C)
	case "callers":
		e.xref("callers", arg, substr, D, xpb.CrossReferencesRequest_CALL_REFERENCES, xpb.CrossReferencesRequest_DIRECT_CALLERS)
	case "super":
		e.inheritance(arg, substr, false)
	case "sub":
		e.inheritance(arg, substr, true)
	case "edges":
		e.edges(arg)
	case "nodes":
		e.nodes(arg)
	case "identifier", "names":
		e.identifier(arg, substr || verb == "names")
	default:
		die("unknown verb %q", verb)
	}
}

func (e *engine) repl() {
	fmt.Fprintln(os.Stderr, "[repl] ready (in-process; verbs: def ref callers super sub edges nodes identifier; ^D to exit)")
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		toks := strings.Fields(line)
		verb := toks[0]
		substr := false
		var arg string
		for _, t := range toks[1:] {
			if t == "--substr" {
				substr = true
			} else if arg == "" {
				arg = t
			}
		}
		if arg == "" && verb != "stat" {
			fmt.Fprintln(os.Stderr, "[repl] usage: <verb> <name> [--substr]")
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[repl] error: %v\n", r)
				}
			}()
			e.dispatch(verb, arg, substr)
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
		if len(rest) < 1 {
			die("%s needs <name|ticket>", verb)
		}
		substr := false
		var arg string
		for _, a := range rest {
			if a == "--substr" {
				substr = true
			} else if arg == "" {
				arg = a
			}
		}
		e.dispatch(verb, arg, substr)
	}
}
