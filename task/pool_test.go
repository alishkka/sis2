package task

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// funcProcessor lets a test decide what "processing" does.
type funcProcessor func(input []float64) (Result, error)

func (f funcProcessor) Process(input []float64) (Result, error) { return f(input) }

// okProcessor returns the first number as the Sum, so a test can see
// that every task got the result of its OWN input.
var okProcessor = funcProcessor(func(in []float64) (Result, error) {
	return Result{Count: len(in), Sum: in[0]}, nil
})

// createTasks makes n tasks; task number i has the input {i+1}.
func createTasks(t *testing.T, svc *Service, n int) []int {
	t.Helper()
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		created, err := svc.Create([]float64{float64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = created.ID
	}
	return ids
}

func newPool(t *testing.T, svc *Service, workers, queue int) *Pool {
	t.Helper()
	p, err := NewPool(svc, workers, queue)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func snapshot(t *testing.T, svc *Service, id int) Task {
	t.Helper()
	got, err := svc.Snapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// runWithTimeout fails the test if f does not return in time.
// It is only a safety net, so a deadlock does not hang `go test` forever.
func runWithTimeout(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout (deadlock?): %s", what)
	}
}

func TestSetStateWithQueued(t *testing.T) {
	tests := []struct {
		from, to State
		wantErr  bool
	}{
		{Pending, Queued, false},
		{Queued, Processing, false},
		{Queued, Pending, false},
		{Queued, Completed, true},
		{Queued, Failed, true},
		{Queued, Queued, true},
		{Processing, Queued, true},
		{Completed, Queued, true},
		{Failed, Queued, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+" to "+string(tt.to), func(t *testing.T) {
			task := &Task{State: tt.from}
			err := task.SetState(tt.to)

			if tt.wantErr && !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("error = %v, want ErrInvalidTransition", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	svc := NewService(StatsProcessor{})
	created, _ := svc.Create([]float64{1, 2, 3})
	svc.Process(created.ID)

	snap := snapshot(t, svc, created.ID)
	snap.State = Failed
	snap.Input[0] = 999
	snap.Result.Sum = -1

	again := snapshot(t, svc, created.ID)
	if again.State != Completed || again.Input[0] != 1 || again.Result.Sum != 6 {
		t.Errorf("storage was changed through a snapshot: %+v", again)
	}
}

func TestSnapshotNotFound(t *testing.T) {
	svc := NewService(StatsProcessor{})
	if _, err := svc.Snapshot(5); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("error = %v, want ErrTaskNotFound", err)
	}
}

func TestNewPoolBadConfig(t *testing.T) {
	svc := NewService(StatsProcessor{})
	if _, err := NewPool(svc, 1, 5); !errors.Is(err, ErrBadPoolConfig) {
		t.Errorf("1 worker: error = %v, want ErrBadPoolConfig", err)
	}
	if _, err := NewPool(svc, 2, 0); !errors.Is(err, ErrBadPoolConfig) {
		t.Errorf("queue 0: error = %v, want ErrBadPoolConfig", err)
	}
}

func TestPoolStartsAndStops(t *testing.T) {
	pool := newPool(t, NewService(StatsProcessor{}), 3, 2)
	// Shutdown returns only after every worker has exited. If a worker never
	// stopped, this would hit the timeout.
	runWithTimeout(t, "shutdown", pool.Shutdown)
}

func TestSubmitDoesNotWait(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		close(started)
		<-release // the worker stays here until the test lets it go
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 1)
	pool := newPool(t, svc, 2, 2)

	if err := pool.Submit(ids[0]); err != nil {
		t.Fatal(err)
	}
	// Submit already returned, but the processor is still blocked:
	// so Submit does not wait for the processing.
	<-started
	if got := snapshot(t, svc, ids[0]).State; got != Processing {
		t.Errorf("State while running = %s, want Processing", got)
	}

	close(release)
	runWithTimeout(t, "shutdown", pool.Shutdown)
	if got := snapshot(t, svc, ids[0]).State; got != Completed {
		t.Errorf("State after = %s, want Completed", got)
	}
}

func TestTwoTasksRunAtTheSameTime(t *testing.T) {
	// Each task waits until the other one is also inside the processor.
	// This can only finish if both run at the same moment.
	var bothInside sync.WaitGroup
	bothInside.Add(2)
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		bothInside.Done()
		bothInside.Wait()
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 2)
	pool := newPool(t, svc, 2, 2)

	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
	runWithTimeout(t, "the two tasks did not overlap", pool.Shutdown)

	for _, id := range ids {
		if got := snapshot(t, svc, id).State; got != Completed {
			t.Errorf("task %d: State = %s, want Completed", id, got)
		}
	}
}

func TestManyTasksGetTheirOwnResult(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 50)
	pool := newPool(t, svc, 4, 50)

	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
	runWithTimeout(t, "shutdown", pool.Shutdown)

	for i, id := range ids {
		got := snapshot(t, svc, id)
		if got.State != Completed {
			t.Errorf("task %d: State = %s, want Completed", id, got.State)
			continue
		}
		if got.Result == nil || got.Result.Sum != float64(i+1) {
			t.Errorf("task %d: Result = %v, want Sum %d", id, got.Result, i+1)
		}
	}
}

func TestRealStatsProcessorInPool(t *testing.T) {
	svc := NewService(StatsProcessor{})
	a, _ := svc.Create([]float64{10, 20, 30})
	b, _ := svc.Create([]float64{4, 1, 3, 2})
	pool := newPool(t, svc, 2, 2)

	pool.Submit(a.ID)
	pool.Submit(b.ID)
	runWithTimeout(t, "shutdown", pool.Shutdown)

	wantA := Result{Count: 3, Min: 10, Max: 30, Sum: 60, Mean: 20, Median: 20}
	wantB := Result{Count: 4, Min: 1, Max: 4, Sum: 10, Mean: 2.5, Median: 2.5}
	if got := snapshot(t, svc, a.ID); got.Result == nil || *got.Result != wantA {
		t.Errorf("task a: Result = %v, want %+v", got.Result, wantA)
	}
	if got := snapshot(t, svc, b.ID); got.Result == nil || *got.Result != wantB {
		t.Errorf("task b: Result = %v, want %+v", got.Result, wantB)
	}
}

func TestFailedAndPanickingTasks(t *testing.T) {
	// input 2 -> error, input 3 -> panic, everything else is fine
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		switch in[0] {
		case 2:
			return Result{}, errors.New("boom")
		case 3:
			panic("oh no")
		}
		return Result{Count: 1, Sum: in[0]}, nil
	}))
	ids := createTasks(t, svc, 6) // inputs 1..6
	pool := newPool(t, svc, 2, 6)

	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
	runWithTimeout(t, "shutdown", pool.Shutdown)

	for _, i := range []int{1, 2} { // tasks with input 2 and 3
		got := snapshot(t, svc, ids[i])
		if got.State != Failed || got.Error == "" || got.Result != nil {
			t.Errorf("task %d: %+v, want Failed with error and no result", ids[i], got)
		}
	}
	// the panic must not kill the workers: the other tasks still finish
	for _, i := range []int{0, 3, 4, 5} {
		if got := snapshot(t, svc, ids[i]).State; got != Completed {
			t.Errorf("task %d: State = %s, want Completed", ids[i], got)
		}
	}
}

func TestSubmitInvalid(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 1)
	pool := newPool(t, svc, 2, 5)

	if err := pool.Submit(999); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("unknown id: error = %v, want ErrTaskNotFound", err)
	}
	if err := pool.Submit(ids[0]); err != nil {
		t.Fatal(err)
	}
	// the task is Queued, Processing or Completed now, but never Pending
	if err := pool.Submit(ids[0]); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("second submit: error = %v, want ErrInvalidTransition", err)
	}
	runWithTimeout(t, "shutdown", pool.Shutdown)
}

func TestSubmitFinishedTasksIsRejected(t *testing.T) {
	// task 1 will fail, task 2 will complete
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		if in[0] == 1 {
			return Result{}, errors.New("boom")
		}
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 2)
	pool := newPool(t, svc, 2, 2)
	pool.Submit(ids[0])
	pool.Submit(ids[1])

	// wait until both are finished without sleeping: Shutdown waits for them,
	// and then we check the rule on a second pool
	runWithTimeout(t, "shutdown", pool.Shutdown)
	pool2 := newPool(t, svc, 2, 2)
	defer pool2.Shutdown()

	for _, id := range ids {
		if err := pool2.Submit(id); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("task %d: error = %v, want ErrInvalidTransition", id, err)
		}
	}
	if got := snapshot(t, svc, ids[0]).State; got != Failed {
		t.Errorf("failed task changed its state to %s", got)
	}
}

func TestProcessRejectsQueuedTask(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 3)
	pool := newPool(t, svc, 2, 1)

	// wait for each worker to take its task, so the queue (size 1) is empty again
	pool.Submit(ids[0])
	<-started
	pool.Submit(ids[1])
	<-started
	pool.Submit(ids[2]) // fills the queue, stays Queued

	// the old synchronous Process must not take a task that is in the queue
	if err := svc.Process(ids[2]); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("Process on a queued task: error = %v, want ErrInvalidTransition", err)
	}
	close(release)
	runWithTimeout(t, "shutdown", pool.Shutdown)
}

func TestSameTaskSubmittedFromManyGoroutinesRunsOnce(t *testing.T) {
	var calls atomic.Int32
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		calls.Add(1)
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 1)
	pool := newPool(t, svc, 4, 10)

	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if pool.Submit(ids[0]) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	runWithTimeout(t, "shutdown", pool.Shutdown)

	if accepted.Load() != 1 {
		t.Errorf("%d submits were accepted, want 1", accepted.Load())
	}
	if calls.Load() != 1 {
		t.Errorf("the processor ran %d times, want 1", calls.Load())
	}
}

func TestQueueFull(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	svc := NewService(funcProcessor(func(in []float64) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{Count: 1}, nil
	}))
	ids := createTasks(t, svc, 4)
	pool := newPool(t, svc, 2, 1) // 2 workers + 1 place in the queue

	// wait for each worker to take its task, so the queue (size 1) is empty again
	pool.Submit(ids[0])
	<-started
	pool.Submit(ids[1])
	<-started // both workers are busy now

	if err := pool.Submit(ids[2]); err != nil { // takes the only place in the queue
		t.Fatalf("third task: %v", err)
	}
	err := pool.Submit(ids[3])
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("fourth task: error = %v, want ErrQueueFull", err)
	}
	if got := snapshot(t, svc, ids[3]).State; got != Pending {
		t.Errorf("rejected task: State = %s, want Pending", got)
	}

	close(release)
	runWithTimeout(t, "shutdown", pool.Shutdown)

	for _, id := range ids[:3] {
		if got := snapshot(t, svc, id).State; got != Completed {
			t.Errorf("task %d: State = %s, want Completed", id, got)
		}
	}
	if got := snapshot(t, svc, ids[3]).State; got != Pending {
		t.Errorf("rejected task was processed anyway: State = %s", got)
	}
}

func TestSubmitAfterShutdown(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 1)
	pool := newPool(t, svc, 2, 2)
	runWithTimeout(t, "shutdown", pool.Shutdown)

	if err := pool.Submit(ids[0]); !errors.Is(err, ErrPoolStopped) {
		t.Errorf("error = %v, want ErrPoolStopped", err)
	}
	if got := snapshot(t, svc, ids[0]).State; got != Pending {
		t.Errorf("State = %s, want Pending", got)
	}
}

func TestShutdownTwice(t *testing.T) {
	pool := newPool(t, NewService(okProcessor), 2, 2)
	runWithTimeout(t, "first shutdown", pool.Shutdown)
	runWithTimeout(t, "second shutdown", pool.Shutdown) // must not panic (double close)
}

func TestShutdownFinishesAcceptedTasks(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 100)
	pool := newPool(t, svc, 2, 100)

	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
	// Shutdown right away, many tasks are probably still in the queue
	runWithTimeout(t, "shutdown", pool.Shutdown)

	for _, id := range ids {
		if got := snapshot(t, svc, id).State; got != Completed {
			t.Errorf("task %d was lost: State = %s", id, got)
		}
	}
}

func TestSubmitAndShutdownAtTheSameTime(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 200)
	pool := newPool(t, svc, 4, 20)

	var mu sync.Mutex
	accepted := make(map[int]bool)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := pool.Submit(id)
			switch {
			case err == nil:
				mu.Lock()
				accepted[id] = true
				mu.Unlock()
			case errors.Is(err, ErrQueueFull), errors.Is(err, ErrPoolStopped):
				// rejected, that is fine
			default:
				t.Errorf("task %d: unexpected error %v", id, err)
			}
		}(id)
	}
	// Shutdown while the submits are running. It must not panic.
	runWithTimeout(t, "shutdown", pool.Shutdown)
	wg.Wait()

	for _, id := range ids {
		got := snapshot(t, svc, id).State
		if accepted[id] && got != Completed {
			t.Errorf("task %d was accepted but State = %s (lost task)", id, got)
		}
		if !accepted[id] && got != Pending {
			t.Errorf("task %d was rejected but State = %s", id, got)
		}
	}
}

// Run with -race: readers use Snapshot and List while workers change tasks.
func TestReadWhileProcessing(t *testing.T) {
	svc := NewService(okProcessor)
	ids := createTasks(t, svc, 100)
	pool := newPool(t, svc, 4, 100)

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					svc.List()
					svc.Snapshot(ids[0])
				}
			}
		}()
	}

	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
	runWithTimeout(t, "shutdown", pool.Shutdown)
	close(stop)
	readers.Wait()

	for _, got := range svc.List() {
		if got.State != Completed {
			t.Errorf("task %d: State = %s, want Completed", got.ID, got.State)
		}
	}
}

func TestCreateFromManyGoroutines(t *testing.T) {
	svc := NewService(okProcessor)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Create([]float64{1}); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	list := svc.List()
	if len(list) != 50 {
		t.Fatalf("got %d tasks, want 50", len(list))
	}
	for i, got := range list {
		if got.ID != i+1 { // IDs must be unique and without gaps
			t.Errorf("list[%d].ID = %d, want %d", i, got.ID, i+1)
		}
	}
}
