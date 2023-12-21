package orbgeojson

import (
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/paulmach/orb/geojson"
)

func FromFlat(f *flat.Feature) (*geojson.Feature, error) {
	return nil, nil
}

func ToFlat(f geojson.Feature) flat.Feature {
	return flat.Feature{}
}

func ToBuilder(b *flatbuffers.Builder, f geojson.Feature) flatbuffers.UOffsetT {
	return 0
}
