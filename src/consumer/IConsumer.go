package consumer

type IConsumer interface {
	Consume(handler func(message string) error)
}