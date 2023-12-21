package orbconvert

import (
	"github.com/gogama/flatgeobuf-convert/convert"
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/paulmach/orb"
)

func FromFlat(f *flat.Feature) (orb.Geometry, error) {
	return nil, nil
}

func FromFlatProps(f *flat.Feature, s flatgeobuf.Schema) (orb.Geometry, convert.Props, error) {
	// s can be either 'f' repeated, or a header.
	return nil, nil, nil
}

func ToFlat(g orb.Geometry) flat.Feature {
	return flat.Feature{}
}

func ToFlatProps(g orb.Geometry, p convert.Props, putSchema bool) flat.Feature {
	// putSchema tells you whether the schema should be echoed into the
	// feature, or omitted.
	return flat.Feature{}
}

func ToBuilder(b flatbuffers.Builder, g orb.Geometry) flatbuffers.UOffsetT {
	return 0
}

func ToBuilderProps(b flatbuffers.Builder, g orb.Geometry, p convert.Props, putSchema bool) flatbuffers.UOffsetT {
	return 0
}
