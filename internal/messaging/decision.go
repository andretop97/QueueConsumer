package messaging

import (
	"errors"
	"fmt"
	"log/slog"
)

type Action int

const (
	Ack Action = iota + 1
	Retry
	DeadLetter
)

func (a Action) String() string {
	switch a {
	case Ack:
		return "Ack"
	case Retry:
		return "Retry"
	case DeadLetter:
		return "DeadLetter"
	default:
		return fmt.Sprintf("Action(%d)", int(a)) // cobre o valor zero e valores inválidos
	}
}

func (a Action) LogValue() slog.Value {
	return slog.StringValue(a.String())
}

func Decide(err error, attempts, maxRetries int) Action {
	if err != nil {
		if errors.Is(err, ErrPermanent) {
			return DeadLetter
		}

		if attempts < maxRetries {
			return Retry
		}

		if attempts >= maxRetries {
			return DeadLetter
		}

	}
	return Ack
}
