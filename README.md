# SIS Project — Checkpoint 2: Concurrent Statistics Calculator

Individual project for *Golang Application Development* (KBTU, Fall 2026).

A small console application written in Go. A **task** is a list of numbers.
The application validates the input, processes the task, and stores the
resulting statistics (count, min, max, sum, mean, median) in the same task.
All data is stored in memory.

Example: input `[10, 20, 30]` → `count=3 min=10 max=30 sum=60 mean=20 median=20`.

**Checkpoint 2** adds concurrent processing: tasks can be submitted to a
**worker pool** with a **bounded queue**. Submitting does not wait for the
result, and many tasks are processed at the same time. Everything from
Checkpoint 1 (creating, validating, direct processing) still works.

## Requirements

Go 1.27

## Run

```bash
go run .
```

The demo first shows the Checkpoint 1 part, then submits 12 tasks to a pool
of 3 workers with a queue of 4 places.

## Checks and tests

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
```

## Project structure

```
main.go             demo of the application (Checkpoint 1 and Checkpoint 2)
task/task.go        Task, State, Result, errors, validation, state changes
task/processor.go   Processor interface and StatsProcessor (statistics calculation)
task/service.go     Service: creating, finding and processing tasks (in-memory map, protected by a mutex)
task/pool.go        Pool: worker goroutines, bounded queue, Submit, Shutdown
task/task_test.go   unit tests (Checkpoint 1)
task/pool_test.go   concurrency tests (Checkpoint 2)
```

## How tasks are created and submitted

```go
svc := task.NewService(task.StatsProcessor{})
t, _ := svc.Create([]float64{10, 20, 30})   // state: Pending

pool, _ := task.NewPool(svc, 3, 4)          // 3 workers, queue of 4
err := pool.Submit(t.ID)                    // returns at once, state: Queued
...
pool.Shutdown()                             // finish accepted tasks, stop workers

got, _ := svc.Snapshot(t.ID)                // read the task (a copy)
```

- `svc.Process(id)` is the old Checkpoint 1 way: it processes the task right
  now in the calling goroutine.
- `pool.Submit(id)` is the new way: the task goes to the queue and a worker
  processes it in the background.
- `svc.Snapshot(id)` and `svc.List()` return **copies** of tasks, so they can be
  used at any time, also while workers are running.
- `svc.Get(id)` returns the real stored task (like in Checkpoint 1). Reading
  its fields is only safe when no pool is working, so use `Snapshot` with a pool.

## How the pool works

```
Submit(id) ──► [ queue (buffered channel of IDs) ] ──► worker 1 ┐
                                                       worker 2 ├─► Service.execute(id)
                                                       worker N ┘
```

1. `Submit` changes the task `Pending -> Queued` and puts its ID into the queue.
2. A free worker takes an ID from the queue and changes the task
   `Queued -> Processing`.
3. The worker runs the processor, then saves the result and sets
   `Completed` (or `Failed`).

- **Worker count:** fixed, chosen in `NewPool` (at least 2). The workers start
  in `NewPool`, stay alive between tasks and are reused. There is no goroutine
  per task.
- **Queue capacity:** fixed, chosen in `NewPool` (at least 1). It is a buffered
  channel `chan int`.
- **Order:** tasks are not guaranteed to finish in the order they were
  submitted, and nothing in the code depends on it.
- If the processor panics, the panic is turned into an error, the task becomes
  `Failed` and the worker keeps working.

### Full queue

When the queue is full, `Submit` does **not** block. It returns `ErrQueueFull`
and the task goes back to `Pending` (it is never marked as `Processing`), so
the caller can try again later. I chose rejecting instead of blocking because a
blocking `Submit` could hang forever if the pool stops.

## Task lifecycle

```
Pending -> Queued -> Processing -> Completed
   |         |            \-> Failed
   |         \-> Pending   (only if the queue is full)
   \-> Processing          (direct processing with Service.Process)
```

| From         | Allowed next states                                  |
|--------------|------------------------------------------------------|
| `Pending`    | `Queued` (Submit), `Processing` (Service.Process)    |
| `Queued`     | `Processing` (a worker took it), `Pending` (queue was full) |
| `Processing` | `Completed`, `Failed`                                |
| `Completed`  | none (final state)                                   |
| `Failed`     | none (final state)                                   |

- `Pending` — the task is created but not submitted yet.
- `Queued` — the task was accepted and waits in the queue for a free worker.
- `Processing` — a worker (or `Service.Process`) is working on the task.
- `Completed` — processing finished successfully and the result is saved.
- `Failed` — an error occurred during processing (for example, the sum of the
  numbers is too large). A failed task is never marked as completed.

Any other transition returns `ErrInvalidTransition`. That is why submitting a
task that is already `Queued`, `Processing`, `Completed` or `Failed` is
rejected, and a task can not be processed twice. There are no retries: a
`Failed` task stays `Failed`.

## Synchronization

- **One `sync.RWMutex` in `Service`** protects the map, `nextID` and every
  `Task` inside the map (protecting only the map would not be enough, because
  the tasks are changed too).
- **State changes are atomic.** The check and the change happen under the same
  lock (`begin` checks that the task is in the expected state and sets
  `Processing` in one step). Two workers can not take the same task, and two
  `Submit` calls for the same task can not both succeed.
- **The lock is not held while processing.** It is taken only to change a
  state or save a result, so workers really run in parallel.
- **Reads return copies** (`Snapshot`, `List`), so readers never see a task
  that is half changed.
- **`Pool.mu`** protects the `closed` flag and makes "check closed + send to the
  queue" one step. `Shutdown` takes the same mutex when it closes the queue,
  so `Submit` can never send on a closed channel.
- **Lock order:** `Pool.mu` is always taken before `Service.mu`, never the
  other way. Workers take only `Service.mu`. So there is no deadlock.

## Worker shutdown

`pool.Shutdown()`:

1. sets `closed = true` and closes the queue (under `Pool.mu`);
2. the workers keep reading the queue until it is empty (`for id := range queue`),
   so every task that was accepted before the shutdown is still processed;
3. waits for all workers with a `sync.WaitGroup`.

- After `Shutdown`, `Submit` returns `ErrPoolStopped` and the task stays `Pending`.
- Calling `Shutdown` again is safe: the queue is closed only once and the call
  just waits again.
- The `Pool` owns the queue channel and only `Shutdown` closes it.

## Errors

| Error                  | When it occurs                                   |
|------------------------|--------------------------------------------------|
| `ErrInvalidInput`      | empty list or invalid number                     |
| `ErrTaskNotFound`      | no task with the given ID                        |
| `ErrInvalidTransition` | forbidden state transition (also: bad Submit)    |
| `ErrProcessingFailed`  | an error occurred during processing              |
| `ErrQueueFull`         | the queue has no free place                      |
| `ErrPoolStopped`       | `Submit` after `Shutdown`                        |
| `ErrBadPoolConfig`     | fewer than 2 workers or queue size below 1       |

Expected errors are returned as `error` values. `panic` is not used.

## Tests

Checkpoint 1 tests are unchanged. Checkpoint 2 tests (`task/pool_test.go`) check:
pool start and stop, `Submit` not waiting for the result, two tasks running at the
same time (each waits for the other, so it only passes if they really overlap),
many tasks with correct results, failed and panicking tasks, invalid and
duplicate submits (also from many goroutines), full queue, submit after
shutdown, double shutdown, no lost tasks on shutdown, `Submit` and `Shutdown` at
the same time, and reading while workers run.

The tests use channels and `WaitGroup` instead of `time.Sleep`. The only
timeouts are a safety net so a deadlock fails the test instead of hanging.

## Design decisions

- **`Processor` interface:** `Service` does not depend on a concrete
  processing algorithm.
- **Queue of IDs:** the queue holds task IDs, not pointers, so the task data
  stays in one place (the `Service`) under one lock.
- **`Service.execute`** is used both by `Process` (Checkpoint 1) and by the
  workers, so the logic of processing is written once.
- **Input slice is copied** when a task is created and again before it goes to
  the processor, so external code cannot change the task's data.
- **Pointer receivers** (`*Task`, `*Service`, `*Pool`) are used where the state
  changes; **value receivers** (`StatsProcessor`) where there is no data to change.
- **IDs** are sequential integers starting from 1.
