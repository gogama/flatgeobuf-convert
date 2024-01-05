package props

import (
	"errors"
	"fmt"
)

var (
	ErrNoColumn               = textErr("no such column")
	ErrNoValue                = textErr("no value for column")
	ErrTypeMismatch           = textErr("type mismatch: value type does not match schema column type")
	errStringSizeOverflowsInt = textErr("string-ish column size prefix overflows int")
	errStringSizeCorrupt      = textErr("string-ish column size prefix is missing or too short")
	errUnknownColumnType      = textErr("unknown column type")
)

const packageName = "props: "

func textErr(text string) error {
	return errors.New(packageName + text)
}

func fmtErr(format string, a ...any) error {
	return fmt.Errorf(packageName+format, a...)
}

func textPanic(text string) {
	panic(packageName + text)
}
