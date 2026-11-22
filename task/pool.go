package task

import (
	"errors"
	"sync"
)

var (
	ErrQueueFull     = errors.New("queue is full")
	ErrPoolStopped   = errors.New("pool is stopped")
	ErrBadPoolConfig = errors.New("pool needs at least 2 workers and queue size of at least 1")
)

// Pool is a fixed number of worker goroutines that take task IDs
// from a bounded queue and process them.
//
// Who owns what:
//   - the Pool owns the queue channel, and only Shutdown closes it;
//   - the Service owns the tasks, and only its methods change them.
type Pool struct {
	svc   *Service
	queue chan int // bounded queue of task IDs
	wg    sync.WaitGroup

	mu     sync.Mutex // protects `closed`; also makes "check closed + send" one step
	closed bool
}

// NewPool starts the workers right away.
// workers is the number of goroutines, queueSize is how many accepted tasks
// can wait for a free worker.
func NewPool(svc *Service, workers, queueSize int) (*Pool, error) {
	if workers < 2 || queueSize < 1 {
		return nil, ErrBadPoolConfig
	}
	p := &Pool{
		svc:   svc,
		queue: make(chan int, queueSize),
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p, nil
}

// Submit puts a Pending task into the queue and returns immediately,
// it does not wait for the processing.
//
// Possible errors:
//
//	ErrPoolStopped       - Shutdown was already called
//	ErrTaskNotFound      - there is no such task
//	ErrInvalidTransition - the task is not Pending (queued, running or finished)
//	ErrQueueFull         - no free place in the queue, the task stays Pending
func (p *Pool) Submit(id int) error {
	// Shutdown takes the same lock, so the queue can not be closed while
	// we send into it. That is why there is no "send on closed channel" panic.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolStopped
	}
	if err := p.svc.markQueued(id); err != nil {
		return err
	}
	select {
	case p.queue <- id:
		return nil
	default:
		// The queue is full. We do not wait, we reject the task
		// and put it back to Pending.
		p.svc.markPending(id)
		return ErrQueueFull
	}
}

// Shutdown stops accepting new tasks, lets the workers finish every task
// that was already accepted, and waits until all workers have exited.
// Calling it again is safe: it only waits again and does nothing else.
func (p *Pool) Shutdown() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()

	// Waiting happens outside the lock, so Submit is not blocked meanwhile.
	p.wg.Wait()
}

func (p *Pool) worker() {
	defer p.wg.Done()
	// The loop ends when the queue is closed AND empty,
	// so tasks that were accepted before Shutdown are not lost.
	for id := range p.queue {
		// The result (or the error) is saved inside the task,
		// so there is nothing more to do with the returned error here.
		p.svc.execute(id, Queued)
	}
}
