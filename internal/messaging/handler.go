package messaging

import "context"

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type HandlerFunc func(ctx context.Context, msg Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg Message) error { return f(ctx, msg) }
