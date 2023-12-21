package props

import (
	"bytes"
	"github.com/gogama/flatgeobuf-convert/interop"
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
	"math"
)

// Props combines a property Schema with a set of property values under
// the Schema.
//
// The Schema is immutable, but the property values are mutable, so you
// can add new property values that are supported by the Schema, update
// existing values, or delete values.
//
// Props is not safe for concurrent use.
type Props struct {
	// flatSchema is the source schema taken from a FlatGeobuf file.
	flatSchema flatgeobuf.Schema
	// fastSchema is the schema used if the properties were created
	// using the schema type from this package.
	fastSchema *Schema
	// data contains the raw properties in FlatGeobuf property format.
	// It may have the zero value if the property set is empty.
	data bytes.Buffer
	// lookup maps from column index to byte index. The index given is
	// the index of the value start, not the index of the column index
	// which precedes it. Therefore, the index will always be positive.
	// A zero value for a given column index indicates there is no
	// property value for that column. Creation of the lookup table is
	// lazy: if lookup is nil, no one has tried to read or write a
	// property yet.
	lookup []int
	// mutable is true if-and-only if this is a mutable property set.
	// An immutable set can be switched to mutable using the mutate
	// function. This requires duplicating the data array.
	mutable bool
}

func PropsFromFlat(schema flatgeobuf.Schema, data []byte) *Props {
	if schema == nil {
		// FIXME: panic
	}
	return &Props{
		flatSchema: schema,
		data:       *bytes.NewBuffer(data),
		mutable:    false,
	}
}

func NewProps(schema *Schema) *Props {
	if schema == nil {
		// FIXME: panic
	}
	return &Props{
		fastSchema: schema,
		mutable:    true,
	}
}

func (p *Props) columnType(col int) flat.ColumnType {
	if p.fastSchema != nil {
		return p.fastSchema.Type(col)
	}
	var columnType flat.ColumnType
	_ = interop.FlatBufferSafe(func() error {
		var obj flat.Column
		if p.flatSchema.Columns(&obj, col) {
			columnType = obj.Type()
		}
		return nil
	})
	return columnType
}

func (p *Props) numColumns() int {
	if p.fastSchema != nil {
		return p.fastSchema.ColumnsLength()
	} else {
		return p.flatSchema.ColumnsLength()
	}
}

func (p *Props) sizeOfValue(col, pos int) int {
	columnType := p.columnType(col)
	switch columnType {
	case flat.ColumnTypeBool:
		return flatbuffers.SizeBool
	case flat.ColumnTypeByte:
		return flatbuffers.SizeInt8
	case flat.ColumnTypeUByte:
		return flatbuffers.SizeUint8
	case flat.ColumnTypeShort:
		return flatbuffers.SizeInt16
	case flat.ColumnTypeUShort:
		return flatbuffers.SizeUint16
	case flat.ColumnTypeInt:
		return flatbuffers.SizeInt32
	case flat.ColumnTypeUInt:
		return flatbuffers.SizeUint32
	case flat.ColumnTypeLong:
		return flatbuffers.SizeInt64
	case flat.ColumnTypeULong:
		return flatbuffers.SizeUint64
	case flat.ColumnTypeFloat:
		return flatbuffers.SizeFloat32
	case flat.ColumnTypeDouble:
		return flatbuffers.SizeFloat64
	case flat.ColumnTypeString, flat.ColumnTypeJson, flat.ColumnTypeBinary, flat.ColumnTypeDateTime:
		rem := uint64(p.data.Len() - pos)
		if rem < flatbuffers.SizeUint32 {
			n := flatbuffers.GetUint32(p.data.Bytes()[pos:])
			rem = rem - uint64(n)
			if rem > 0 && n <= math.MaxInt-flatbuffers.SizeUint32 {
				return int(n) + flatbuffers.SizeUint32
			}
		}
		fallthrough
	default:
		return p.data.Len() - pos
	}
}

func (p *Props) index2Index(col int) (int, error) {
	n := p.numColumns()
	if col < 0 || col >= n {
		return 0, ErrNoColumn
	} else if p.lookup != nil {
		return p.lookup[col], nil
	} else if p.mutable {
		return 0, nil
	} else {
		p.lookup = make([]int, n)
		i := 0
		for i < p.data.Len()-flatbuffers.SizeUint16 {
			j := flatbuffers.GetUint16(p.data.Bytes()[i:])
			i += flatbuffers.SizeUint16
			if int(j) >= n {
				continue
			}
			sz := p.sizeOfValue(int(j), i)
			if i+sz > p.data.Len() {
				break
			}
			p.lookup[j] = i
			i += sz
		}
		return p.lookup[col], nil
	}
}

func (p *Props) name2Col(name string) (int, error) {
	if p.fastSchema != nil {
		if col, ok := p.fastSchema.Index(name); ok {
			return col, nil
		}
		return 0, ErrNoColumn
	} else {
		n := p.flatSchema.ColumnsLength()
		var col int
		err := interop.FlatBufferSafe(func() error {
			var obj flat.Column
		columns:
			for i := 0; i < n; i++ {
				if p.flatSchema.Columns(&obj, i) {
					b := obj.Name()
					if len(b) != len(name) {
						continue columns
					}
					for j := range b {
						if b[j] != name[j] {
							break columns
						}
					}
					col = i
					return nil
				}
			}
			return ErrNoColumn
		})
		return col, err
	}
}

func (p *Props) name2Index(name string) (int, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.index2Index(col)
}

func (p *Props) mutate() {
	if !p.mutable {
		data := make([]byte, p.data.Len())
		copy(data, p.data.Bytes())
		p.data = *bytes.NewBuffer(data)
		p.mutable = true
	}
}

// minCap is the minimum capacity of a newly-allocated data slice.
const minCap = 64

func (p *Props) extend(col, n int) []byte {
	if p.lookup[col] != 0 {
		// FIXME: Panic here, it's a logic error.
	} else if col > math.MaxUint16 {
		// FIXME: Panic here, it's a logic error.
	} else if n > math.MaxInt-flatbuffers.SizeUint16 {
		// FIXME: Panic here, it's a logic error.
	}
	p.mutate()
	p.data.Grow(flatbuffers.SizeUint16 + n)
	b := p.data.Bytes()
	flatbuffers.WriteUint16(b, uint16(n))
	i := len(b) + flatbuffers.SizeUint16
	p.lookup[col] = i
	return b[i:]
}

func (p *Props) delete(col, pos int) {
	p.mutate()
	sz := p.sizeOfValue(col, pos)
	if pos+sz < p.data.Len() {
		b := p.data.Bytes()
		copy(b[pos-flatbuffers.SizeUint16:], b[pos+sz:])
	}
	p.data.Truncate(pos - flatbuffers.SizeUint16)
	p.lookup[col] = 0
}

func (p *Props) Schema() *Schema {
	return nil
}

func (p *Props) Has(index int) bool {
	i, err := p.index2Index(index)
	return err != nil && i > 0
}

func (p *Props) HasName(name string) bool {
	i, err := p.name2Index(name)
	return err != nil && i > 0
}

func (p *Props) Delete(index int) {
	i, err := p.index2Index(index)
	if err == nil && i > 0 {
		p.delete(index, i)
	}
}

func (p *Props) DeleteName(name string) {
	index, err := p.name2Col(name)
	if err == nil {
		p.Delete(index)
	}
}

func (p *Props) Value(index int) (any, error) {
	// TODO: Convenience method if ppl want to deal with reflection
	return nil, nil
}

func (p *Props) ValueName(name string) (any, error) {
	// TODO: Convenience method if ppl want to deal with reflection
	return nil, nil
}

func (p *Props) SetValue(index int, value any) error {
	// TODO: Convenience method if ppl want to deal with reflection
	return nil
}

func (p *Props) SetValueName(name string, value any) error {
	// TODO: Convenience method if ppl want to deal with reflection
	return nil
}

func (p *Props) Byte(index int) (int8, error) {
	i, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if i == 0 {
		return 0, ErrNoValue
	}
	return int8(p.data.Bytes()[i]), nil
}

func (p *Props) ByteName(name string) (int8, error) {
	i, err := p.name2Index(name)
	if err != nil {
		return 0, err
	} else if i == 0 {
		return 0, ErrNoValue
	}
	return int8(p.data.Bytes()[i]), nil
}

func (p *Props) SetByte(index int, value int8) error {
	i, err := p.index2Index(index)
	if err != nil {
		return err
	}
	var b []byte
	if i > 0 {
		b = p.data.Bytes()[i:]
	} else {
		b = p.extend(index, flatbuffers.SizeInt8)
	}
	b[0] = byte(value)
	return nil
}

func (p *Props) SetByteNamed(name string, value int8) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	i, _ := p.index2Index(col)
	var b []byte
	if i > 0 {
		b = p.data.Bytes()[i:]
	} else {
		b = p.extend(col, flatbuffers.SizeInt8)
	}
	b[0] = byte(value)
	return nil
}
