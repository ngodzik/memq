package memq

import "container/ring"

// AMQP package implements a simple message using go channels to communicate.

// NewMessageBroker returns a new message broker instance.
// The new message broker will receive (bufferedSize) messages before the call to send in a publisher blocks.
func NewMessageBroker(bufferedSize int) *Broker {
	b := &Broker{publisherQueues: make(map[string]chan message),
		publishersNb:  make(map[string]int),
		subscribers:   make(map[string]*ring.Ring),
		subscribersNb: make(map[string]int),
		bufferedSize:  bufferedSize,
		wait:          make(chan struct{})}
	return b
}
