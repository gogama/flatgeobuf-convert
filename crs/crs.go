package crs

import (
	"github.com/gogama/flatgeobuf-convert/interop"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
)

type CRS struct {
	Org         string
	Code        int32
	Name        string
	Description string
	WKT         string
	CodeString  string
}

func FromFlat(crs *flat.Crs) (*CRS, error) {
	var result CRS
	err := interop.FlatBufferSafe(func() error {
		result.Org = string(crs.Org())
		result.Code = crs.Code()
		result.Name = string(crs.Name())
		result.Description = string(crs.Description())
		result.WKT = string(crs.Wkt())
		result.CodeString = string(crs.CodeString())
		return nil
	})
	return &result, err
}

func (crs *CRS) ToBuilder(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	func() {
		if crs.Org != "" {
			offset := b.CreateString(crs.Org)
			defer flat.CrsAddOrg(b, offset)
		}
		defer flat.CrsAddCode(b, crs.Code)
		if crs.Name != "" {
			offset := b.CreateString(crs.Name)
			defer flat.CrsAddName(b, offset)
		}
		if crs.Description != "" {
			offset := b.CreateString(crs.Description)
			defer flat.CrsAddDescription(b, offset)
		}
		if crs.WKT != "" {
			offset := b.CreateString(crs.WKT)
			defer flat.CrsAddWkt(b, offset)
		}
		if crs.CodeString != "" {
			offset := b.CreateString(crs.CodeString)
			defer flat.CrsAddWkt(b, offset)
		}
		flat.CrsStart(b)
	}()
	return flat.CrsEnd(b)
}
