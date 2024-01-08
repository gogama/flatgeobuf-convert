package props

import (
	"github.com/gogama/flatgeobuf-convert/interop"
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
)

// Compile-time checks.
var (
	_ flatgeobuf.Schema = &Schema{}
)

type Schema struct {
	cols       []Column
	name2Index map[string]int
}

func SchemaFromFlat(obj flatgeobuf.Schema) (schema *Schema, err error) {
	var cols []Column
	err = interop.FlatBufferSafe(func() error {
		n := obj.ColumnsLength()
		cols = make([]Column, n)
		var col flat.Column
		for i := range cols {
			if !obj.Columns(&col, i) {
				// FIXME: Missing indicated column: return error
			}
			var err error
			if cols[i], err = ColumnFromFlat(&col); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Schema{
		cols: cols,
	}, nil
}

func NewSchema(cols []Column) *Schema {
	return &Schema{
		cols: cols,
	}
}

const name2IndexThreshold = 6

func (s *Schema) Index(name string) (index int, ok bool) {
	if s.name2Index != nil {
		index, ok = s.name2Index[name]
	} else if len(s.cols) < name2IndexThreshold {
		for i := range s.cols {
			if s.cols[i].Name == name {
				index, ok = i, true
				break
			}
		}
	} else {
		s.name2Index = make(map[string]int, len(s.cols))
		for i := range s.cols {
			if s.cols[i].Name == name {
				index, ok = i, true
			}
			s.name2Index[s.cols[i].Name] = i
		}
	}
	return
}

func (s *Schema) Name(index int) (name string) {
	if index < 0 || index >= len(s.cols) {
		return ""
	}
	return s.cols[index].Name
}

func (s *Schema) Type(index int) flat.ColumnType {
	var colType flat.ColumnType
	if 0 <= index && index < len(s.cols) {
		colType = s.cols[index].Type
	}
	return colType
}

func (s *Schema) Column(index int) (col Column) {
	if 0 <= index && index < len(s.cols) {
		col = s.cols[index]
	}
	return
}

func (s *Schema) ColumnsLength() int {
	return len(s.cols)
}

func (s *Schema) Columns(obj *flat.Column, j int) bool {
	if j < 0 || j >= len(s.cols) {
		return false
	}
	b := flatbuffers.NewBuilder(64)
	offset := s.cols[j].ToBuilder(b)
	obj.Init(b.FinishedBytes(), offset)
	return true
}

func (s *Schema) ToBuilder(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	n := len(s.cols)
	offsets := make([]flatbuffers.UOffsetT, n)
	for i := range offsets {
		offsets[i] = s.cols[i].ToBuilder(b)
	}
	// flat.HeaderStartColumnsVector and flat.FeatureStartColumnsVector
	// are functionally equivalent. We could use either of them, and
	// arbitrarily choose the header one.
	flat.HeaderStartColumnsVector(b, len(s.cols))
	for i := range offsets {
		b.PrependUOffsetT(offsets[n-i-1])
	}
	return b.EndVector(n)
}
