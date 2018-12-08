package memq

type topicSub struct {
	priority uint
	action   func(msg interface{}) (err error)
	channel  chan interface{}
}

type Subscriber struct {
	//	byTopic    map[string]*topicSub
	//	byPriority []*topicSub
	topic *topicSub

	isBlocking bool

	//TODO
	debug string
}

func (b *Broker) NewSubscriber(isBlocking bool) *Subscriber {
	return &Subscriber{isBlocking: isBlocking}
}

// AddTopic adds a topic to subscribe with a giver priority.
// The lower the priority number is, the higher the priority is.
// Let's note that there is a chance that when two messages arrive at exactly
// the same time that one message with lower priority is processed before.
// It should rarely happen, however, if it is a strong requirement, a dedicated
// implementation using no golang channels should be chosen.
func (b *Broker) AddTopic(subscriber *Subscriber, keyTopic string, priority uint, action func(msg interface{}) (err error)) {

	t := &topicSub{priority: priority, action: action, channel: make(chan interface{}, 1)}

	subscriber.topic = t

	subscriber.debug = keyTopic
	//subscriber.byPriority = append(subscriber.byPriority, t)

	// Sort topics by priority, will be useful to manage priorities after

	b.addSubscriber(keyTopic, subscriber)

	// Sort byPriority array
	//	sort.SliceStable(subscriber.byPriority, func(i, j int) bool { return subscriber.byPriority[i].priority < subscriber.byPriority[j].priority })
}

// BlockingRead waits until a message is ready to be read.
func (s *Subscriber) WaitAndProcessMessage() (bool, error) {

	// All the messages cannot be read in the same select with the priority management, because in the case of several channels can be read, the case will be selected randomly.
	//TODO

	if !s.isBlocking {
		//		for i := range s.byPriority {
		select {
		case msg, ok := <-s.topic.channel:
			if ok == false {
				return false, nil
			}
			return true, s.topic.action(msg.(message).value)
		default:
			return false, nil
		}
	} else {
		select {
		case msg, ok := <-s.topic.channel:
			if ok == false {
				return false, nil
			}
			return true, s.topic.action(msg.(message).value)
		}
	}

	//TODO
	// We saw that the highest priorities channels was empty. Select now on all the channels.
	//var cases []reflect.SelectCase
	//for i := range s.byPriority {
	//	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.byPriority[i].channel)})
	//}
	//got, msg, ok := reflect.Select(cases)
	//		if ok {
	//	return true, s.byPriority[got].action(msg.Interface().(message).value) //TODO.(*message).value)
	//} else {
	//	return false, nil
	//}
	//	}

	return false, nil
}
