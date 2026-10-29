package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

type Middleware func(Handler) Handler

func Chain(h Handler, mws ...Middleware) Handler {
	for _, mw := range slices.Backward(mws) {
		h = mw(h)
	}
	return h
}

func Recover() Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(
			func(ctx context.Context, msg Message) (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = panicError(r)
					}
				}()
				return next.Handle(ctx, msg)
			})
	}

}

func panicError(r any) error {
	if e, ok := r.(error); ok {
		return fmt.Errorf("%w: %w: %w", ErrPanic, ErrPermanent, e)
	}
	return fmt.Errorf("%w: %w: %v", ErrPanic, ErrPermanent, r)
}

func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return HandlerFunc(
			func(ctx context.Context, msg Message) error {
				start := time.Now()
				err := next.Handle(ctx, msg)

				attrs := []slog.Attr{
					slog.Int("attempts", msg.Attempts),
					slog.Int("body_bytes", len(msg.Body)),
					slog.Duration("duration", time.Since(start)),
				}
				if err != nil {
					attrs = append(attrs, slog.String("error", err.Error()))
					logger.LogAttrs(ctx, slog.LevelError, "falha ao processar mensagem", attrs...)
					return err
				}
				logger.LogAttrs(ctx, slog.LevelInfo, "mensagem processada", attrs...)
				return nil
			})
	}

}
