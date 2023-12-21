package props

import "errors"

var (
	ErrNoColumn = textErr("no such column")
	ErrNoValue  = textErr("no value for column")
)

const packageName = "props: "

func textErr(text string) error {
	return errors.New(packageName + text)
}
