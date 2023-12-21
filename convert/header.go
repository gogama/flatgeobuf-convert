package convert

import "github.com/gogama/flatgeobuf/flatgeobuf"

// Compile-time checks.
var (
	_ flatgeobuf.Schema = &Header{}
)

type Header struct {
	Schema
}
