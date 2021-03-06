package beat

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"encoding/json"
	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"strings"
)

const (
	tsOutput = "2015-12-29T13:55:15.000Z"
)

func TestCanStartAndStopBeat(t *testing.T) {
	rb, b, err := helpBuildBeat("./testfiles/minimal.yml")

	if err != nil {
		fmt.Printf("2. err = %v, %T\n", err, err)
		assert.NotNil(t, err, err.Error())
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		rb.Run(b)
		wg.Done()
	}()

	time.AfterFunc(1*time.Second, func() {
		rb.Stop()
	})
	wg.Wait()
}

func TestConfigIsLoaded(t *testing.T) {
	rb, _, _ := helpBuildBeat("./testfiles/minimal.yml")
	assert.NotNil(t, rb.Config)
	assert.Equal(t, 1, len(*rb.RbConfig.AmqpInput.Channels))
	assert.Equal(t, "test", *(*rb.RbConfig.AmqpInput.Channels)[0].Name)
}

func TestExtractTsFormats(t *testing.T) {

	type test struct {
		tsField  string
		tsFormat string
		tsValue  string
		expected string
	}
	tests := [...]test{
		test{"field1", "2006-01-02 15:04:05 -0700", "2015-12-29 14:55:15 +0100", tsOutput},
		test{"field2", "January 02, 2006 03:04:05PM -0700", "December 29, 2015 01:55:15PM -0000", tsOutput},
		test{"field3", "20060102150405", "20151229135515", tsOutput},
		test{"field4", "01/02 `06, 03:04:05.00000 -0700", "12/29 `15, 06:55:15.00000 -0700", tsOutput},
	}

	for _, tst := range tests {
		m := make(common.MapStr)
		m[tst.tsField] = tst.tsValue
		ts, err := extractTS(m, tst.tsField, tst.tsFormat, common.Time(time.Now()))
		assert.Nil(t, err)
		bytes, _ := ts.MarshalJSON()
		str := string(bytes)
		assert.Equal(t, tst.expected, strings.Trim(str, "\""))
	}
}

func TestNestedTsField(t *testing.T) {
	m := common.MapStr{
		"foo": map[string]interface{}{
			"bar": map[string]interface{}{
				"tsfield": "December 29, 2015 01:55:15PM -0000",
			},
		},
	}

	ts, err := extractTS(m, "foo[>]bar[>]tsfield", "January 02, 2006 03:04:05PM -0700", common.Time(time.Now()))
	assert.Nil(t, err)
	bytes, _ := ts.MarshalJSON()
	str := string(bytes)
	assert.Equal(t, tsOutput, strings.Trim(str, "\""))
}

func TestSanitizeKey(t *testing.T) {
	tests := map[string]string {
		"___abcd_efg": "abcd_efg",
		"_abcd_efg": "abcd_efg",
		"_.abc.def.ghi.": "abc_def_ghi_",
		".abc.def.": "abc_def_",
	}

	cfg := &AmqpConfig{}
	cfg.SetDefaults()

	for input, expected := range tests {
		assert.Equal(t, expected, sanitizeKey(input, cfg))
	}
}

func TestSanitize(t *testing.T) {
	m := common.MapStr {
		"_.a.b." : common.MapStr {
			"_._def.xyz_": common.MapStr {
				"__test.foo.": "_._._._.",
			},
		},
	}

	k0 := "a_b_"
	k1 := "def_xyz_"
	k2 := "test_foo_"

	cfg := &AmqpConfig{}
	cfg.SetDefaults()
	sanitize(m, cfg)

	assert.Equal(t, 1, len(m))
	_, ok := m[k0]; assert.True(t, ok)
	m0 := m[k0].(common.MapStr)

	_, ok = m0[k1]; assert.True(t, ok)
	m1 := m0[k1].(common.MapStr)
	assert.Equal(t, 1, len(m1))

	_, ok = m1[k2]; assert.True(t, ok)
	s := m1[k2].(string)
	assert.Equal(t, "_._._._.", s)
}

func TestSanitizeEdgeCases(t *testing.T) {
	m := common.MapStr {
		"_.a.b" : "collision1",
		"a_b" : "collision2",
		"___" : "empty",
	}

	cfg := &AmqpConfig{}
	cfg.SetDefaults()
	sanitize(m, cfg)

	for k, v := range m {
		if v == "collision1" {
			assert.Equal(t, 0, strings.Index(k, "a_b"))
			assert.Equal(t, 3+1+36, len(k))
		}
		if v == "collision2" {
			assert.Equal(t, "a_b", k)
		}
		if v == "empty" {
			assert.Equal(t, 36, len(k))
		}
	}
}

func TestSingleMessage(t *testing.T) {

	expected := "This is a test"
	data := struct {
		Payload string        `json:"payload"`
		Nested  common.MapStr `json:"nested"`
	}{
		Payload: expected,
		Nested: common.MapStr{
			"tsfield": "December 29, 2015 01:55:15PM -0000",
		},
	}

	test, err := json.Marshal(data)
	assert.Nil(t, err)

	received := false
	rb, b, _ := helpBuildBeat("./testfiles/full.yml")
	c0 := &(*rb.RbConfig.AmqpInput.Channels)[0]
	c0.TsField = new(string)
	*c0.TsField = "nested[>]tsfield"
	c0.TsFormat = new(string)
	*c0.TsFormat = "January 02, 2006 03:04:05PM -0700"

	pub := newPublisher(*rb.RbConfig.AmqpInput.ServerURI, &(*rb.RbConfig.AmqpInput.Channels)[0], nil)
	defer pub.close()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	b.Events = &MockClient{beat: rb,
		eventsPublished: func(events []common.MapStr, beat *AmqpBeat) {
			received = true
			assert.Equal(t, expected, events[0]["payload"])
			ct, _ := events[0]["@timestamp"].(common.Time)
			js, _ := ct.MarshalJSON()
			assert.Equal(t, tsOutput, strings.Trim(string(js), "\""))
			wg.Done()
		},
	}

	go rb.Run(b)
	pub.send(string(test))

	wg.Wait()
	rb.Stop()
	assert.True(t, received, "Did not receive message")
}

type Runner struct {
	t        *testing.T
	ab       *AmqpBeat
	b        *beat.Beat
	received chan common.MapStr
	pubs     []*Publisher
	pubConn  *amqp.Connection
	spec     Spec
}

func newRunner(t *testing.T, config string) *Runner {
	ab, b, _ := helpBuildBeat(config)
	r := &Runner{
		ab:       ab,
		b:        b,
		received: make(chan common.MapStr),
	}
	r.initPublishers()

	b.Events = &MockClient{beat: ab,
		eventsPublished: func(events []common.MapStr, beat *AmqpBeat) {
			for _, event := range events {
				r.received <- event
			}
		},
		eventPublished: func(event common.MapStr, beat *AmqpBeat) {
			r.received <- event
		},
	}

	return r
}

func (r *Runner) initPublishers() {
	var ch *amqp.Channel
	input := r.ab.RbConfig.AmqpInput
	r.pubs = make([]*Publisher, len(*input.Channels))
	for i, cfg := range *input.Channels {
		pub := newPublisher(*input.ServerURI, &cfg, ch)
		if pub.conn != nil {
			r.pubConn = pub.conn
		}
		r.pubs[i] = pub
	}
}

type Spec interface {
	numQueues() int
	handleEvent(e common.MapStr)
	isComplete() bool
	verify(runner *Runner)
}

func (r *Runner) run() {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for c := range r.received {
			r.spec.handleEvent(c)
			if r.spec.isComplete() {
				break
			}
		}

		r.spec.verify(r)
		r.ab.Stop()
		wg.Done()
	}()

	go r.ab.Run(r.b)

	wg.Wait()

}

func (r *Runner) cleanup() error {
	return r.pubConn.Close()
}

type MultiQueueTest struct {
	msgPerQueue   int
	totalExpected int
	counts        map[string]int
	totalReceived int
}

func (test *MultiQueueTest) init() {
	test.msgPerQueue = 1000
	test.totalExpected = test.msgPerQueue * test.numQueues()
	test.counts = make(map[string]int)
	test.totalReceived = 0
}

func (test *MultiQueueTest) numQueues() int {
	return 4
}

func (test *MultiQueueTest) handleEvent(e common.MapStr) {
	typeName := e["type"].(string)
	test.counts[typeName]++
	test.totalReceived++
}

func (test *MultiQueueTest) isComplete() bool {
	return test.totalReceived >= test.totalExpected
}

func (test *MultiQueueTest) verify(r *Runner) {
	for i := 0; i < len(r.pubs); i++ {
		assert.Equal(r.t, test.msgPerQueue, test.counts[r.pubs[i].typeTag], "Did not receive all messages for publisher")
	}
}

func TestCanReceiveOnMultipleQueues(t *testing.T) {
	r := newRunner(t, "./testfiles/multi.yml")
	defer r.cleanup()
	test := new(MultiQueueTest)
	test.init()
	r.spec = test
	testMsg := "{\"payload\": \"This is a tetst\"}"
	for i := 0; i < test.totalExpected; i++ {
		r.pubs[i%len(r.pubs)].send(testMsg)
	}
	r.run()
}

func helpBuildBeat(cfgFile string) (*AmqpBeat, *beat.Beat, error) {
	rb := new(AmqpBeat)
	b := beat.NewBeat("", "", rb)
	b.Events = &MockClient{beat: rb,
		eventsPublished: func(event []common.MapStr, beat *AmqpBeat) {
		},
	}
	err := rb.ConfigWithFile(b, cfgFile)
	if err != nil {
		panic(err)
	}
	err = rb.Setup(b)
	if err != nil {
		panic(err)
	}
	return rb, b, nil
}

type Publisher struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	exch       string
	routingKey string
	typeTag    string
}

/*
If ch is nil, a new Connection and Channel will be created, and this publisher
will 'own' the connection. A call to close() will close both channel and connection.

If ch is provided, then this publisher will reuse the channel and
calls to close() will do nothing.
*/
func newPublisher(serverURI string, cfg *ChannelConfig, ch *amqp.Channel) *Publisher {

	var conn *amqp.Connection
	var err error

	if ch == nil {
		conn, err = amqp.Dial(serverURI)
		if err != nil {
			panic(fmt.Errorf("Failed to connect to RabbitMQ: %v", err))
		}

		ch, err = conn.Channel()
		if err != nil {
			panic(fmt.Errorf("Failed to open a channel: %v", err))
		}
	}

	_, err = ch.QueueDeclare(*cfg.Name, *cfg.Durable, *cfg.AutoDelete, *cfg.Exclusive, false, *cfg.Args)
	if err != nil {
		panic(fmt.Errorf("Failed to declare queue %s: %v", cfg.Name, err))
	}

	ch.QueuePurge(*cfg.Name, true)
	return &Publisher{exch: "", routingKey: *cfg.Name, conn: conn, ch: ch, typeTag: *cfg.TypeTag}
}

func (p *Publisher) close() {
	// Only close if this instance was responsible for creating the connection
	if p.conn != nil {
		defer p.conn.Close()
		defer p.ch.Close()
	}
}

func (p *Publisher) send(msg string) {
	err := (*p.ch).Publish("", p.routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg),
	})

	if err != nil {
		panic(fmt.Errorf("Failed to publish message: %v", err))
	}
}

type MockClient struct {
	beat            *AmqpBeat
	eventPublished  func(event common.MapStr, beat *AmqpBeat)
	eventsPublished func(event []common.MapStr, beat *AmqpBeat)
	lastReceived    time.Time
	msgsReceived    int
}

func (c MockClient) PublishEvent(event common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventPublished(event, c.beat)
	c.lastReceived = time.Now()
	return true
}

func (c MockClient) PublishEvents(events []common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventsPublished(events, c.beat)
	c.lastReceived = time.Now()
	return true
}
