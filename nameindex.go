package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"kythe.io/kythe/go/storage/stream"
	"kythe.io/kythe/go/util/kytheuri"
	"kythe.io/kythe/go/util/markedsource"

	cpb "kythe.io/kythe/proto/common_go_proto"
	spb "kythe.io/kythe/proto/storage_go_proto"
)

// nameIndex is a sorted (name, ticket) table — the same on-disk format scry3
// writes (TAB-separated, one pair per line), so scry3 and scry4 share index
// files interchangeably.
type nameRow struct{ name, ticket string }
type nameIndex struct{ rows []nameRow }

func loadNameIndex(path string) (*nameIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rows []nameRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, '\t'); i > 0 {
			rows = append(rows, nameRow{line[:i], line[i+1:]})
		}
	}
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return rows[i].name < rows[j].name }) {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	}
	return &nameIndex{rows: rows}, nil
}

func (ni *nameIndex) lookup(name string, substr bool) []string {
	if substr {
		var out []string
		for _, p := range ni.substr(name) {
			out = append(out, p.ticket)
		}
		return out
	}
	lo := sort.Search(len(ni.rows), func(i int) bool { return ni.rows[i].name >= name })
	var out []string
	for i := lo; i < len(ni.rows) && ni.rows[i].name == name; i++ {
		out = append(out, ni.rows[i].ticket)
	}
	return out
}

func (ni *nameIndex) substr(needle string) []nameRow {
	var out []nameRow
	for _, p := range ni.rows {
		if strings.Contains(p.name, needle) {
			out = append(out, p)
			if len(out) >= 50 {
				break
			}
		}
	}
	return out
}

// stripJVMDesc drops a trailing JVM method descriptor: foo()V → foo.
func stripJVMDesc(sig string) (string, bool) {
	open := strings.LastIndexByte(sig, '(')
	if open < 0 {
		return "", false
	}
	close := strings.IndexByte(sig[open:], ')')
	if close < 0 {
		return "", false
	}
	close += open
	ret := sig[close+1:]
	if ret == "" {
		return "", false
	}
	return sig[:open], true
}

// buildNameIndex scans every <sha>.entries file for the two name carriers —
// /kythe/edge/named (Java/JVM/Go) and /kythe/code MarkedSource (C++) — using
// Kythe's own stream reader, markedsource renderer, and kytheuri (no
// hand-rolled proto/MarkedSource parsing, unlike scry3). Output is the same
// sorted name<TAB>ticket file scry3 produces.
func buildNameIndex(entriesDir, out string) {
	files, err := filepath.Glob(filepath.Join(entriesDir, "*.entries"))
	if err != nil || len(files) == 0 {
		die("no *.entries in %s", entriesDir)
	}
	set := make(map[nameRow]struct{})
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scry4: skip %s: %v\n", path, err)
			continue
		}
		rd := stream.NewReader(f)
		_ = rd(func(e *spb.Entry) error {
			switch {
			case e.GetEdgeKind() == "/kythe/edge/named" && e.GetTarget().GetSignature() != "":
				ticket := kytheuri.ToString(e.GetSource())
				sig := e.GetTarget().GetSignature()
				set[nameRow{sig, ticket}] = struct{}{}
				if p, ok := stripJVMDesc(sig); ok {
					set[nameRow{p, ticket}] = struct{}{}
				}
			case e.GetFactName() == "/kythe/code" && len(e.GetFactValue()) > 0:
				var ms cpb.MarkedSource
				if proto.Unmarshal(e.GetFactValue(), &ms) == nil {
					if si := markedsource.RenderQualifiedName(&ms); si.GetQualifiedName() != "" {
						set[nameRow{si.GetQualifiedName(), kytheuri.ToString(e.GetSource())}] = struct{}{}
					}
				}
			}
			return nil
		})
		f.Close()
	}
	rows := make([]nameRow, 0, len(set))
	for r := range set {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].name != rows[j].name {
			return rows[i].name < rows[j].name
		}
		return rows[i].ticket < rows[j].ticket
	})
	w, err := os.Create(out)
	if err != nil {
		die("create %s: %v", out, err)
	}
	bw := bufio.NewWriter(w)
	for _, r := range rows {
		fmt.Fprintf(bw, "%s\t%s\n", r.name, r.ticket)
	}
	bw.Flush()
	w.Close()
	fmt.Fprintf(os.Stderr, "scry4: name index: %d rows → %s\n", len(rows), out)
}
