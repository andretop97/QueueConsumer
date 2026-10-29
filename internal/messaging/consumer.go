package messaging

import "context"

type Consumer interface {
	Consume(ctx context.Context, h Handler) error
}
