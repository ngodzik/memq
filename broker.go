package memq

import (
	"container/ring"
	"sync"
)

type Broker struct {
	// topics defines one channel by topic, all the publisher that publish to a topic use this channel.
	publisherQueues map[string]chan message //TODOPublisher

	// Number of publisher by topic
	publishersNb map[string]int //TODOPublisher

	// Subcribers, one ring by topic, a publisher can be in several rings if it registered to several topics.
	subscribers   map[string]*ring.Ring
	subscribersNb map[string]int

	// We are using the same mutex for adding subscribers and publishers.
	// As initiliazation is done at the beginning of the program,
	// it should not be necessary to use several mutexes to try to gain performance.
	mutex sync.Mutex

	// The size for the buffered publisher channels.
	bufferedSize int

	// The Broker go routines has to stop if activated
	wait chan struct{}
}

type message struct {
	value interface{}

	pubType pType
}

// Publisher message type
type pType = uint

const (
	_ pType = iota
	// Auto dispatches message automatically to the scheduled subscriber
	Auto

	// Multicast messages to all the subscribers, will be read after remaining automatic messages.
	Multicast
)

// Start function starts the message broker.
func (b *Broker) Start() {

	// The broker uses one goroutine per topic registered.
	for k, v := range b.subscribers {

		go func(topic string, ch chan message, ring *ring.Ring) {

		Working:
			for {
				msg, ok := <-ch
				if ok == false {
					// Iterate through the ring.
					ring.Do(func(i interface{}) {
						subscriber := i.(*Subscriber)
						close(subscriber.topic.channel)
					})
					break Working
				}

				if msg.pubType == Multicast {
					ring.Do(func(i interface{}) {
						subscriber := i.(*Subscriber)
						subscriber.topic.channel <- msg
					})
				} else {
					subscriber := ring.Value.(*Subscriber)
					subscriber.topic.channel <- msg
					ring = ring.Next()
				}
			}

		}(k, b.publisherQueues[k], v)
	}
}

func (b *Broker) addSubscriber(topic string, s *Subscriber) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if _, ok := b.subscribers[topic]; !ok {
		r := ring.New(1)
		r.Value = s
		b.subscribers[topic] = r
		b.subscribersNb[topic] = 1
	} else {
		rTopic := b.subscribers[topic]

		r := ring.New(1)
		r.Value = s

		b.subscribers[topic] = rTopic.Link(r)
		b.subscribersNb[topic]++
	}
}

// getPublisherQueues will return the dedicated channel or create it if the topic key does not exist.
func (b *Broker) getPublisherQueue(topic string) chan message {
	b.mutex.Lock()
	defer func() {
		b.mutex.Unlock()
	}()

	var ch chan message

	if _, ok := b.publisherQueues[topic]; !ok {
		// The list for this topic doesn't exist, creating it
		ch = make(chan message, b.bufferedSize)
		b.publisherQueues[topic] = ch
		b.publishersNb[topic] = 1
	} else {
		ch = b.publisherQueues[topic]
		b.publishersNb[topic]++
	}

	return ch
}

func (b *Broker) addPublisher(topic string, nb int) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.publishersNb[topic] += nb
}

func (b *Broker) rmPublisher(topic string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.publishersNb[topic]--
	if b.publishersNb[topic] == 0 {
		close(b.publisherQueues[topic])
	}
}
