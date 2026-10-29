package messaging

import "errors"

var ErrPermanent = errors.New("permanent")
var ErrPanic = errors.New("panic no handler")
