package messaging

type Message struct {
	Body     []byte
	Headers  map[string]any
	Attempts int
}
