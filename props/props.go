package props

import (
	"bytes"
	"errors"
	"math"
	"time"
	"unsafe"

	"github.com/gogama/flatgeobuf-convert/interop"
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
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
		textPanic("nil schema")
	}
	return &Props{
		flatSchema: schema,
		data:       *bytes.NewBuffer(data),
		mutable:    false,
	}
}

func NewProps(schema *Schema) *Props {
	if schema == nil {
		textPanic("nil schema")
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

func (p *Props) sizeOfValue(col, pos int) (int, error) {
	columnType := p.columnType(col)
	switch columnType {
	case flat.ColumnTypeBool:
		return flatbuffers.SizeBool, nil
	case flat.ColumnTypeByte:
		return flatbuffers.SizeInt8, nil
	case flat.ColumnTypeUByte:
		return flatbuffers.SizeUint8, nil
	case flat.ColumnTypeShort:
		return flatbuffers.SizeInt16, nil
	case flat.ColumnTypeUShort:
		return flatbuffers.SizeUint16, nil
	case flat.ColumnTypeInt:
		return flatbuffers.SizeInt32, nil
	case flat.ColumnTypeUInt:
		return flatbuffers.SizeUint32, nil
	case flat.ColumnTypeLong:
		return flatbuffers.SizeInt64, nil
	case flat.ColumnTypeULong:
		return flatbuffers.SizeUint64, nil
	case flat.ColumnTypeFloat:
		return flatbuffers.SizeFloat32, nil
	case flat.ColumnTypeDouble:
		return flatbuffers.SizeFloat64, nil
	case flat.ColumnTypeString, flat.ColumnTypeJson, flat.ColumnTypeBinary, flat.ColumnTypeDateTime:
		rem := uint64(p.data.Len() - pos)
		if rem > flatbuffers.SizeUint32 {
			n := uint64(flatbuffers.GetUint32(p.data.Bytes()[pos:]))
			rem = rem - n
			if n <= math.MaxInt-flatbuffers.SizeUint32 {
				return int(n) + flatbuffers.SizeUint32, nil
			}
			return 0, errStringSizeOverflowsInt
		}
		return 0, errStringSizeCorrupt
	default:
		return 0, errUnknownColumnType
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
			sz, err := p.sizeOfValue(int(j), i)
			if err != nil || i+sz > p.data.Len() {
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
	if !p.mutable {
		// FIXME: Panic here, it's a logic error
	} else if p.lookup[col] != 0 {
		// FIXME: Panic here, it's a logic error.
	} else if col > math.MaxUint16 {
		// FIXME: Panic here, it's a logic error.
	} else if n > math.MaxInt-flatbuffers.SizeUint16 {
		// FIXME: Panic here, it's a logic error.
	}
	p.data.Grow(flatbuffers.SizeUint16 + n)
	b := p.data.Bytes()
	flatbuffers.WriteUint16(b, uint16(n))
	i := len(b) + flatbuffers.SizeUint16
	p.lookup[col] = i
	return b[i:]
}

func (p *Props) delete(col, pos int) {
	p.mutate()
	sz, err := p.sizeOfValue(col, pos)
	if err != nil {
		return
	}
	if pos+sz < p.data.Len() {
		b := p.data.Bytes()
		copy(b[pos-flatbuffers.SizeUint16:], b[pos+sz:])
	}
	p.data.Truncate(pos - flatbuffers.SizeUint16)
	p.lookup[col] = 0
}

func (p *Props) check(col int, expectedType flat.ColumnType) error {
	actualType := p.columnType(col)
	if actualType != expectedType {
		return ErrTypeMismatch
	}
	return nil
}

func (p *Props) Schema() *Schema {
	// TODO: Do we want FlatSchema and Schema? The former would always have a return value.
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

func (p *Props) Delete(index int) bool {
	i, err := p.index2Index(index)
	if err == nil || i == 0 {
		return false
	}
	p.delete(index, i)
	return true
}

func (p *Props) DeleteName(name string) bool {
	index, err := p.name2Col(name)
	if err != nil {
		return false
	}
	return p.Delete(index)
}

func (p *Props) GetValue(index int) (any, error) {
	columnType := p.columnType(index)
	switch columnType {
	case flat.ColumnTypeBool:
		return p.GetBool(index)
	case flat.ColumnTypeByte:
		return p.GetByte(index)
	case flat.ColumnTypeUByte:
		return p.GetUByte(index)
	case flat.ColumnTypeShort:
		return p.GetShort(index)
	case flat.ColumnTypeUShort:
		return p.GetUShort(index)
	case flat.ColumnTypeInt:
		return p.GetInt(index)
	case flat.ColumnTypeUInt:
		return p.GetUInt(index)
	case flat.ColumnTypeLong:
		return p.GetLong(index)
	case flat.ColumnTypeULong:
		return p.GetULong(index)
	case flat.ColumnTypeFloat:
		return p.GetFloat(index)
	case flat.ColumnTypeDouble:
		return p.GetDouble(index)
	case flat.ColumnTypeString:
		return p.GetString(index)
	case flat.ColumnTypeJson:
		return p.GetJSON(index)
	case flat.ColumnTypeBinary:
		return p.GetBinary(index)
	case flat.ColumnTypeDateTime:
		v, err := p.GetDateTime(index)
		if errors.As(err, &time.ParseError{}) {
			v, err = p.GetDateTimeString(index)
		}
		return v, err
	default:
		return nil, errInvalidColumnType(columnType)
	}
}

func (p *Props) GetValueName(name string) (any, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return nil, err
	}
	return p.GetValue(col)
}

func (p *Props) SetValue(index int, value any) error {
	switch v := value.(type) {
	case bool:
		return p.SetBool(index, v)
	case int8:
		return p.SetByte(index, v)
	case uint8:
		return p.SetUByte(index, v)
	case int16:
		return p.SetShort(index, v)
	case uint16:
		return p.SetUShort(index, v)
	case int32:
		return p.SetInt(index, v)
	case uint32:
		return p.SetUInt(index, v)
	case int64:
		return p.SetLong(index, v)
	case uint64:
		return p.SetULong(index, v)
	case float32:
		return p.SetFloat(index, v)
	case float64:
		return p.SetDouble(index, v)
	case string:
		return p.SetString(index, v)
	case []byte:
		return p.SetBinary(index, v)
	case time.Time:
		return p.SetDateTime(index, v)
	default:
		return fmtErr("value %v of type %T does not map to a FlatGeobuf column type", value, v)
	}
}

func (p *Props) SetValueName(name string, value any) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetValue(col, value)
}

func (p *Props) GetBool(index int) (bool, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return false, err
	} else if err = p.check(index, flat.ColumnTypeBool); err != nil {
		return false, err
	} else if pos == 0 {
		return false, ErrNoValue
	}
	return p.data.Bytes()[pos] != 0, nil
}

func (p *Props) GetBoolName(name string) (bool, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return false, err
	}
	return p.GetBool(col)
}

func (p *Props) SetBool(index int, value bool) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeBool); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeBool)
	}
	var v byte
	if value {
		v = 1
	}
	b[0] = v
	return nil
}

func (p *Props) SetBoolName(name string, value bool) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetBool(col, value)
}

func (p *Props) GetByte(index int) (int8, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeByte); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return int8(p.data.Bytes()[pos]), nil
}

func (p *Props) GetByteName(name string) (int8, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetByte(col)
}

func (p *Props) SetByte(index int, value int8) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeByte); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeInt8)
	}
	b[0] = byte(value)
	return nil
}

func (p *Props) SetByteName(name string, value int8) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetByte(col, value)
}

func (p *Props) GetUByte(index int) (uint8, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeUByte); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return p.data.Bytes()[pos], nil
}

func (p *Props) GetUByteName(name string) (uint8, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetUByte(col)
}

func (p *Props) SetUByte(index int, value uint8) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeUByte); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeUint8)
	}
	b[0] = value
	return nil
}

func (p *Props) SetUByteName(name string, value uint8) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetUByte(col, value)
}

func (p *Props) GetShort(index int) (int16, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeShort); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetInt16(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetShortName(name string) (int16, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetShort(col)
}

func (p *Props) SetShort(index int, value int16) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeShort); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeInt16)
	}
	flatbuffers.WriteInt16(b, value)
	return nil
}

func (p *Props) SetShortName(name string, value int16) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetShort(col, value)
}

func (p *Props) GetUShort(index int) (uint16, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeUShort); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetUint16(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetUShortName(name string) (uint16, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetUShort(col)
}

func (p *Props) SetUShort(index int, value uint16) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeUShort); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeUint16)
	}
	flatbuffers.WriteUint16(b, value)
	return nil
}

func (p *Props) SetUShortName(name string, value uint16) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetUShort(col, value)
}

func (p *Props) GetInt(index int) (int32, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeInt); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetInt32(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetIntName(name string) (int32, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetInt(col)
}

func (p *Props) SetInt(index int, value int32) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeInt); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeInt32)
	}
	flatbuffers.WriteInt32(b, value)
	return nil
}

func (p *Props) SetIntName(name string, value int32) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetInt(col, value)
}

func (p *Props) GetUInt(index int) (uint32, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeUInt); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetUint32(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetUIntName(name string) (uint32, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetUInt(col)
}

func (p *Props) SetUInt(index int, value uint32) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeUInt); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeUint32)
	}
	flatbuffers.WriteUint32(b, value)
	return nil
}

func (p *Props) SetUIntName(name string, value uint32) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetUInt(col, value)
}

func (p *Props) GetLong(index int) (int64, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeLong); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetInt64(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetLongName(name string) (int64, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetLong(col)
}

func (p *Props) SetLong(index int, value int64) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeLong); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeInt64)
	}
	flatbuffers.WriteInt64(b, value)
	return nil
}

func (p *Props) SetLongName(name string, value int64) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetLong(col, value)
}

func (p *Props) GetULong(index int) (uint64, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeULong); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetUint64(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetULongName(name string) (uint64, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetULong(col)
}

func (p *Props) SetULong(index int, value uint64) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeULong); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeUint64)
	}
	flatbuffers.WriteUint64(b, value)
	return nil
}

func (p *Props) SetULongName(name string, value uint64) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetULong(col, value)
}

func (p *Props) GetFloat(index int) (float32, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeFloat); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetFloat32(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetFloatName(name string) (float32, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetFloat(col)
}

func (p *Props) SetFloat(index int, value float32) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeFloat); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeFloat32)
	}
	flatbuffers.WriteFloat32(b, value)
	return nil
}

func (p *Props) SetFloatName(name string, value float32) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetFloat(col, value)
}

func (p *Props) GetDouble(index int) (float64, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return 0, err
	} else if err = p.check(index, flat.ColumnTypeDouble); err != nil {
		return 0, err
	} else if pos == 0 {
		return 0, ErrNoValue
	}
	return flatbuffers.GetFloat64(p.data.Bytes()[pos:]), nil
}

func (p *Props) GetDoubleName(name string) (float64, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return 0, err
	}
	return p.GetDouble(col)
}

func (p *Props) SetDouble(index int, value float64) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, flat.ColumnTypeDouble); err != nil {
		return err
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
	} else {
		b = p.extend(index, flatbuffers.SizeFloat64)
	}
	flatbuffers.WriteFloat64(b, value)
	return nil
}

func (p *Props) SetDoubleName(name string, value float64) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetDouble(col, value)
}

func (p *Props) getBinary(index int, columnType flat.ColumnType) ([]byte, error) {
	pos, err := p.index2Index(index)
	if err != nil {
		return nil, err
	} else if err = p.check(index, columnType); err != nil {
		return nil, err
	} else if pos == 0 {
		return nil, ErrNoValue
	}
	b := p.data.Bytes()[pos:]
	n := uint64(flatbuffers.GetUint32(b))
	if n > math.MaxInt-flatbuffers.SizeUint32 {
		return nil, errStringSizeOverflowsInt
	}
	return b[flatbuffers.SizeUint32 : flatbuffers.SizeUint32+n], nil
}

func (p *Props) setBinary(index int, columnType flat.ColumnType, value []byte) error {
	pos, err := p.index2Index(index)
	if err != nil {
		return err
	} else if err = p.check(index, columnType); err != nil {
		return err
	} else /* IF ... TODO: Do an overflow check on this branch. */ {
	}
	var b []byte
	p.mutate()
	if pos > 0 {
		b = p.data.Bytes()[pos:]
		n := flatbuffers.GetUint32(b)
		if n != uint32(len(value)) {
			b = nil
			p.delete(index, pos) // Length changed. Delete and re-extend.
		}
	}
	if b == nil {
		b = p.extend(index, len(value))
	}
	flatbuffers.WriteUint32(b, uint32(len(value)))
	copy(b[flatbuffers.SizeUint32:], value)
	return nil
}

func (p *Props) GetString(index int) (string, error) {
	b, err := p.getBinary(index, flat.ColumnTypeString)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Props) GetStringName(name string) (string, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return "", err
	}
	return p.GetString(col)
}

func (p *Props) SetString(index int, value string) error {
	return p.setBinary(index, flat.ColumnTypeString, unsafe.Slice(unsafe.StringData(value), len(value)))
}

func (p *Props) SetStringName(name string, value string) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetString(col, value)
}

func (p *Props) GetJSON(index int) (string, error) {
	b, err := p.getBinary(index, flat.ColumnTypeJson)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Props) GetJSONName(name string) (string, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return "", err
	}
	return p.GetJSON(col)
}

func (p *Props) SetJSON(index int, value string) error {
	return p.setBinary(index, flat.ColumnTypeJson, unsafe.Slice(unsafe.StringData(value), len(value)))
}

func (p *Props) SetJSONName(name string, value string) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetJSON(col, value)
}

func (p *Props) GetBinary(index int) ([]byte, error) {
	b, err := p.getBinary(index, flat.ColumnTypeBinary)
	if err != nil {
		return nil, err
	}
	dup := make([]byte, len(b))
	copy(dup, b)
	return dup, nil
}

func (p *Props) GetBinaryName(name string) ([]byte, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return nil, err
	}
	return p.GetBinary(col)
}

func (p *Props) SetBinary(index int, value []byte) error {
	return p.setBinary(index, flat.ColumnTypeBinary, value)
}

func (p *Props) SetBinaryName(name string, value []byte) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetBinary(col, value)
}

// TODO: Will also need a version of this to get date/time as string, not time.Time, in case parse fails.

func (p *Props) GetDateTime(index int) (time.Time, error) {
	b, err := p.getBinary(index, flat.ColumnTypeDateTime)
	if err != nil {
		return time.Time{}, err
	}
	s := unsafe.String(&b[0], len(b)) // Temporary unsafe string pointing into buffer.
	return time.Parse(time.RFC3339, s)
}

func (p *Props) GetDateTimeName(name string) (time.Time, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return time.Time{}, err
	}
	return p.GetDateTime(col)
}

func (p *Props) SetDateTime(index int, value time.Time) error {
	s := value.Format(time.RFC3339) // TODO: Use our own format string????
	ptr := unsafe.StringData(s)
	b := unsafe.Slice(ptr, len(s))
	return p.setBinary(index, flat.ColumnTypeDateTime, b)
}

func (p *Props) SetDateTimeName(name string, value time.Time) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetDateTime(col, value)
}

func (p *Props) GetDateTimeString(index int) (string, error) {
	b, err := p.getBinary(index, flat.ColumnTypeDateTime)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Props) GetDateTimeStringName(name string) (string, error) {
	col, err := p.name2Col(name)
	if err != nil {
		return "", err
	}
	return p.GetDateTimeString(col)
}

func (p *Props) SetDateTimeString(index int, value string) error {
	return p.setBinary(index, flat.ColumnTypeDateTime, unsafe.Slice(unsafe.StringData(value), len(value)))
}

func (p *Props) SetDateTimeStringName(name string, value string) error {
	col, err := p.name2Col(name)
	if err != nil {
		return err
	}
	return p.SetDateTimeString(col, value)
}

func (p *Props) String() string {
	// TODO: Implement string-ification.
}
