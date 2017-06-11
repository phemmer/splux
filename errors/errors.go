package errors

import (
	"fmt"
	"io"
)

type Error struct {
	NestedErr error
	Err       error
}

func (e Error) Error() string {
	return e.Err.Error() + ": " + e.NestedErr.Error()
}

func Contains(err error, thing error) bool {
	if err == thing {
		return true
	}
	if err, ok := err.(Error); ok {
		if err.Err == thing {
			return true
		}
		return Contains(err.NestedErr, thing)
	}
	return false
}

func IsEOF(err error) bool {
	return Contains(err, io.EOF)
}

func F(err error, msg string, format ...interface{}) error {
	if err == nil {
		return nil
	}
	return Error{
		NestedErr: err,
		Err:       fmt.Errorf(msg, format...),
	}
}
