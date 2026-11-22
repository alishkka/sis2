package task

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrInvalidInput      = errors.New("invalid input")
	ErrTaskNotFound      = errors.New("task not found")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrProcessingFailed  = errors.New("processing failed")
)

type State string

const (
	Pending    State = "Pending"
	Queued     State = "Queued"
	Processing State = "Processing"
	Completed  State = "Completed"
	Failed     State = "Failed"
)

type Result struct {
	Count  int
	Min    float64
	Max    float64
	Sum    float64
	Mean   float64
	Median float64
}

type Task struct {
	ID     int
	Input  []float64
	State  State
	Result *Result
	Error  string
}

func canChange(from, to State) bool {
	switch from {
	case Pending:
		// Processing: direct processing (Service.Process, Checkpoint 1)
		// Queued: the task was accepted by the pool (Pool.Submit)
		return to == Queued || to == Processing
	case Queued:
		// Pending: only when the queue turned out to be full
		return to == Processing || to == Pending
	case Processing:
		return to == Completed || to == Failed
	default:
		return false
	}
}

// SetState is NOT safe for concurrent use by itself.
// The Service calls it only while holding its lock.
func (t *Task) SetState(next State) error {
	if !canChange(t.State, next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.State, next)
	}
	t.State = next
	return nil
}

func ValidateInput(values []float64) error {
	if len(values) == 0 {
		return fmt.Errorf("%w: list of values is empty", ErrInvalidInput)
	}
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w: value %v is not a finite number", ErrInvalidInput, v)
		}
	}
	return nil
}
