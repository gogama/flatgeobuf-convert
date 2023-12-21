package header

import (
	"github.com/gogama/flatgeobuf-convert/crs"
	"github.com/gogama/flatgeobuf-convert/props"
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
)

// Compile-time checks.
var (
	_ flatgeobuf.Schema = &Header{}
)

type Header struct {
	Name          *string
	Envelope      []float64
	GeometryType  flat.GeometryType
	HasZ          bool
	HasM          bool
	HasT          bool
	HasTM         bool
	Schema        *props.Schema
	FeaturesCount uint64
	IndexNodeSize *uint16
	CRS           *crs.CRS
	Title         *string
	Description   *string
	Metadata      *string
}

func FromFlat(hdr flat.Header) *Header {
	return nil // TODO
}

func (hdr *Header) ToFlat() *flat.Header {
	return &flat.Header{} // TODO
}
func (hdr *Header) ToBuilder(b flatbuffers.Builder) flatbuffers.UOffsetT {
	return 0 // TODO
}

func (hdr *Header) ColumnsLength() int {
	return hdr.ColumnsLength() // TODO: is there a nil case?
}

func (hdr *Header) Columns(obj *flat.Column, j int) bool {
	return hdr.Columns(obj, j) // TODO: is there a nil case?
}
