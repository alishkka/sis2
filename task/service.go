package task

import (
	"fmt"
	"sort"
	"sync"
)

// Service keeps all tasks in memory.
//
// Everything that is shared (the map, nextID and every Task inside the map)
// is protected by one RWMutex. The lock is held only for short steps
// (change a state, save a result), never while the processor is working.
type Service struct {
	mu        sync.RWMutex
	tasks     map[int]*Task
	nextID    int
	processor Processor
}

func NewService(p Processor) *Service {
	return &Service{
		tasks:     make(map[int]*Task),
		nextID:    1,
		processor: p,
	}
}

func (s *Service) Create(input []float64) (*Task, error) {
	if err := ValidateInput(input); err != nil {
		return nil, err
	}

	data := make([]float64, len(input))
	copy(data, input)

	s.mu.Lock()
	defer s.mu.Unlock()
	t := &Task{ID: s.nextID, Input: data, State: Pending}
	s.tasks[t.ID] = t
	s.nextID++
	return t, nil
}

// Get returns the real task from the storage (not a copy), like in Checkpoint 1.
// Reading its fields is only safe when no worker pool is running.
// While a Pool is working, use Snapshot instead.
func (s *Service) Get(id int) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
	}
	return t, nil
}

// Snapshot returns a copy of the task. It is safe to call at any time,
// also while the task is being processed.
func (s *Service) Snapshot(id int) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
	}
	return copyTask(t), nil
}

// List returns copies of all tasks, sorted by ID.
func (s *Service) List() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, copyTask(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// copyTask makes a deep copy (own Input slice and own Result).
// The caller must hold the lock.
func copyTask(t *Task) Task {
	c := *t
	c.Input = append([]float64(nil), t.Input...)
	if t.Result != nil {
		r := *t.Result
		c.Result = &r
	}
	return c
}

// Process processes a Pending task right now in the calling goroutine
// (Checkpoint 1 way). For background processing use Pool.Submit.
func (s *Service) Process(id int) error {
	return s.execute(id, Pending)
}

// execute is used by Process and by the pool workers.
// It moves the task from `from` to Processing, runs the processor
// WITHOUT holding the lock, and then saves the result.
func (s *Service) execute(id int, from State) error {
	input, err := s.begin(id, from)
	if err != nil {
		return err
	}
	result, procErr := s.runProcessor(input)
	return s.finish(id, result, procErr)
}

// begin checks the current state and changes it to Processing in one locked
// step. Two goroutines can not both win, so a task is never processed twice.
func (s *Service) begin(id int, from State) ([]float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
	}
	if t.State != from {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.State, Processing)
	}
	if err := t.SetState(Processing); err != nil {
		return nil, err
	}
	return append([]float64(nil), t.Input...), nil
}

// runProcessor turns a panic inside the processor into an error,
// so one bad task can not kill a worker goroutine.
func (s *Service) runProcessor(input []float64) (result Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor panic: %v", r)
		}
	}()
	return s.processor.Process(input)
}

func (s *Service) finish(id int, result Result, procErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[id]
	if procErr != nil {
		t.Error = procErr.Error()
		t.SetState(Failed)
		return fmt.Errorf("%w: %v", ErrProcessingFailed, procErr)
	}
	t.Result = &result
	return t.SetState(Completed)
}

// markQueued: Pending -> Queued (atomic, so only one Submit can win).
func (s *Service) markQueued(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
	}
	return t.SetState(Queued)
}

// markPending: Queued -> Pending (used when the queue is full).
func (s *Service) markPending(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("%w: id %d", ErrTaskNotFound, id)
	}
	return t.SetState(Pending)
}
