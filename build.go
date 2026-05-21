package main

import (
	"context"
	"fmt"
	"os"

	"kythe.io/kythe/go/serving/pipeline"
	"kythe.io/kythe/go/storage/leveldb"

	spb "kythe.io/kythe/proto/storage_go_proto"
)

// buildServing turns a GraphStore (LevelDB, as produced by `scry3
// index-stream --keep-graphstore` or `write_entries`) into a serving table —
// IN-PROCESS via Kythe's serving pipeline, no `write_tables` subprocess.
func buildServing(graphstore, out string) {
	ctx := context.Background()
	if _, err := os.Stat(out); err == nil {
		die("serving table %q already exists; remove it first", out)
	}
	gs, err := leveldb.OpenGraphStore(graphstore, &leveldb.Options{MustExist: true})
	if err != nil {
		die("open graphstore %q: %v", graphstore, err)
	}
	defer gs.Close(ctx)

	_ = os.MkdirAll(out, 0755)
	db, err := leveldb.Open(out, nil)
	if err != nil {
		die("create serving table %q: %v", out, err)
	}
	defer db.Close(ctx)

	rd := func(f func(*spb.Entry) error) error {
		return gs.Scan(ctx, &spb.ScanRequest{}, f)
	}
	fmt.Fprintf(os.Stderr, "scry4: building serving table %s from graphstore %s …\n", out, graphstore)
	if err := pipeline.Run(ctx, rd, db, &pipeline.Options{
		MaxPageSize:  4000,
		MaxShardSize: 32000,
	}); err != nil {
		die("pipeline: %v", err)
	}
	fmt.Fprintf(os.Stderr, "scry4: serving table built → %s\n", out)
}
