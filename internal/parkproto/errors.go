package parkproto

import "errors"

var (
	ErrVersionMismatch = errors.New("park protocol version mismatch")
	ErrMalformed       = errors.New("park protocol malformed frame")
	ErrSequence        = errors.New("park protocol sequence error")
	ErrTruncated       = errors.New("park protocol truncated frame")
	ErrOversized       = errors.New("park protocol oversized frame")
	ErrBinding         = errors.New("park protocol release binding mismatch")
)
