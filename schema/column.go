package schema

import (
	"github.com/gogama/flatgeobuf-convert/interop"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	flatbuffers "github.com/google/flatbuffers/go"
)

type Column struct {
	Name        string
	Type        flat.ColumnType
	Title       string
	Description string
	Width       int32
	Precision   int32
	Scale       int32
	Required    bool // Opposite of nullable, so zero value matches expectation.
	Unique      bool
	PrimaryKey  bool
	Metadata    string
}

func ColumnFromFlat(obj *flat.Column) (col Column, err error) {
	err = interop.FlatBufferSafe(func() error {
		col.Name = string(obj.Name())
		col.Type = obj.Type()
		col.Title = string(obj.Title())
		col.Description = string(obj.Description())
		col.Width = obj.Width()
		col.Precision = obj.Precision()
		col.Scale = obj.Scale()
		col.Required = !obj.Nullable()
		col.Unique = obj.Unique()
		col.PrimaryKey = obj.PrimaryKey()
		col.Metadata = string(obj.Metadata())
		return nil
	})
	return
}

func (c *Column) ToBuilder(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	func() {
		offset := b.CreateString(c.Name)
		defer func() {
			flat.ColumnAddName(b, offset)
			flat.ColumnAddType(b, c.Type)
			flat.ColumnAddNullable(b, !c.Required)
			flat.ColumnAddUnique(b, c.Unique)
			flat.ColumnAddPrimaryKey(b, c.PrimaryKey)
		}()
		if c.Title != "" {
			offset := b.CreateString(c.Title)
			defer flat.ColumnAddTitle(b, offset)
		}
		if c.Description != "" {
			offset := b.CreateString(c.Description)
			defer flat.ColumnAddDescription(b, offset)
		}
		if c.Metadata != "" {
			offset := b.CreateString(c.Metadata)
			defer flat.ColumnAddMetadata(b, offset)
		}
		flat.ColumnStart(b)
	}()
	return flat.ColumnEnd(b)
}
