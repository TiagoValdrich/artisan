package artisan

import "errors"

var (
	ErrNilHandler = errors.New("handler must not be nil")
)
