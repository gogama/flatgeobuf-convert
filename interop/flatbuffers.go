package interop

import (
	"fmt"
)

func FlatBufferSafe(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: flatbuffers: %v", r)
		}
	}()
	err = f()
	return
}
