package convert

import (
	"github.com/gogama/flatgeobuf/flatgeobuf"
	"time"
)

// props is structured internally so that the data array can be written
// directly to file and contains a meaningful FlatGeobuf property slice.
type props struct {
	schema Schema
	// data is a contiguous byte array big enough to hold all the
	// fixed-size (i.e. number) column values, followed by all the
	// variable-size (i.e. binary, string, date) values. It is stored
	// in FlatGeobuf property value format already, so it can be
	// directly written to file.
	//
	// For mutable, it starts nil and can grow lazily.
	//
	// All the fixed-size (i.e. number) column values come first, in an
	// order decided by the Schema. They are prefixed with their Schema
	// column index. The Schema has a secret function you can use to
	// look up the value's start index.
	//
	// All the variable size values come afterward, in the order they
	// were added to Props. Deleting or updating a variable-length
	// property value can be [relatively] expensive. Since they come in
	// FlatGeobuf property format, they are prefixed by their column
	// number and length.
	data []byte
}

// Props is the immutable version read from file. It is meant to be a
// no-copy structure, so props.data can be directly copied from the FGB
// feature properties.
type Props interface {
	GetByteIndex(col int) int8
	GetByteName(col string) int8
	GetUByteIndex(col int) uint8
	GetUByteName(col string) uint8
	// TODO: Add GetInt, GetUInt, GetFloat, GetDouble, GetString, etc.
	GetDateTimeIndex(col int) time.Time // I/O is via time.Time, internal is string
	GetDateTimeString(col string) time.Time
}

type MutableProps interface {
	Props
	SetByteIndex(col int, value int8)
	SetByteName(col string, value int8)
	// TODO...
}

func NewProps(schema flatgeobuf.Schema, data []byte) Props {
	// TODO: Check that schema isn't nil.
	// TODO: This function should be able to use just a regular flatgeobuf.Schema
	//       to make life as easy as possible for users, so they don't first
	//       have to convert their Header/Feature to a convert.Schema.

	// Create an immutable properties bag. It takes ownership of `data`
	// and just saves a reference in props.data.
	return &Props{
		props: props{
			data: data,
		},
	}
}

func (p *props) Mutate() MutableProps {
	return nil // Convert it to mutable props
}

type mutableProps struct {
	props
}

func NewMutableProps(schema Schema) MutableProps {
	return nil
}
