module github.com/fiveapplesonthetable/scry4

go 1.22

require (
	google.golang.org/protobuf v1.31.0
	kythe.io v0.0.0
)

require (
	bitbucket.org/creachadair/stringset v0.0.11 // indirect
	github.com/JohannesKaufmann/html-to-markdown v1.4.1 // indirect
	github.com/apache/beam v2.31.0+incompatible // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/codesearch v1.2.0 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/orderedcode v0.0.1 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/sergi/go-diff v1.3.1 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sync v0.4.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231009173412-8bfb1ae86b6c // indirect
	google.golang.org/grpc v1.58.3 // indirect
)

// Deep integration: build against a local Kythe v0.0.75 source checkout.
// Clone kythe next to this repo (../kythe-source) or edit this path.
replace kythe.io => ../kythe-source
