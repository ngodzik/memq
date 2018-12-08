package memq

import (
	"sync"
)

type Publisher struct {
	queue chan message

	// Keep the current number of publisher for a topic, so that a channel is not closed until some publishers are using it.
	number int

	mutex sync.Mutex
}

// AddPublisher adds a publisher to the list managed by the broker.
func (b *Broker) GetPublisher(topic string) *Publisher {

	return &Publisher{queue: b.getPublisherQueue(topic)}
}

func (p *Publisher) Send(pubType pType, msg interface{}) {
	p.queue <- message{pubType: pubType, value: msg}
}

func (b *Broker) ClosePublisher(topic string) {
	b.rmPublisher(topic)
}
