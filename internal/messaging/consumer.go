package messaging

type Consumer interface {
	Consume(handler func(message string) error)
}
