package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testMiddlewareCalled bool
var failMiddlewareCalled bool

func testMiddleware(queue string, next JobFunc) JobFunc {
	return func(message *Msg) error {
		testMiddlewareCalled = true
		return next(message)
	}
}

func failMiddleware(queue string, next JobFunc) JobFunc {
	return func(message *Msg) error {
		failMiddlewareCalled = true
		next(message)
		message.ack = false
		return errors.New("test error")
	}
}

func confirm(manager *manager) (msg *Msg) {
	time.Sleep(10 * time.Millisecond)

	select {
	case msg = <-manager.confirm:
	default:
	}

	return
}

func TestNewWorker(t *testing.T) {
	setupTestConfig()
	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("myqueue", testJob, 1)

	worker := newWorker(manager)
	assert.Equal(t, manager, worker.manager)
}

func TestWork(t *testing.T) {
	setupTestConfig()

	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("myqueue", testJob, 1)

	worker := newWorker(manager)
	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

	//calls job with message args
	go worker.work(messages)
	messages <- message

	args, _ := (<-processed).Array()
	<-manager.confirm

	assert.Equal(t, 2, len(args))
	assert.Equal(t, "foo", args[0])
	assert.Equal(t, "bar", args[1])

	worker.quit()

	//confirms job completed
	go worker.work(messages)
	messages <- message

	<-processed
	assert.Equal(t, message, confirm(manager))

	worker.quit()
}

func TestWork_middlewares(t *testing.T) {
	setupTestConfig()

	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	// runs defined middleware and confirms
	mids := DefaultMiddlewares().Append(testMiddleware)

	manager := newManager("myqueue", testJob, 1, mids...)

	worker := newWorker(manager)
	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

	go worker.work(messages)
	messages <- message

	<-processed
	assert.Equal(t, message, confirm(manager))
	assert.True(t, testMiddlewareCalled)

	worker.quit()
}

func TestFailMiddleware(t *testing.T) {
	setupTestConfig()

	var processed = make(chan *Args)
	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	//doesn't confirm if middleware cancels acknowledgement
	mids := DefaultMiddlewares().Append(failMiddleware)

	manager := newManager("myqueue", testJob, 1, mids...)
	worker := newWorker(manager)
	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

	go worker.work(messages)
	messages <- message

	<-processed
	assert.Nil(t, confirm(manager))
	assert.True(t, failMiddlewareCalled)

	worker.quit()
}

func TestRecoverWithPanic(t *testing.T) {
	setupTestConfig()

	//recovers and confirms if job panics
	var panicJob = (func(message *Msg) error {
		panic(errors.New("AHHHHHHHHH"))
	})

	manager := newManager("myqueue", panicJob, 1)
	worker := newWorker(manager)

	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"],\"retry\":true}")

	go worker.work(messages)
	messages <- message

	assert.Equal(t, message, confirm(manager))

	worker.quit()
}

func TestRecoverWithError(t *testing.T) {
	setupTestConfig()

	//recovers and confirms if job panics
	var panicJob = (func(message *Msg) error {
		return errors.New("AHHHHHHHHH")
	})

	manager := newManager("myqueue", panicJob, 1)
	worker := newWorker(manager)

	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"],\"retry\":true}")

	go worker.work(messages)
	messages <- message

	assert.Equal(t, message, confirm(manager))

	worker.quit()
}
