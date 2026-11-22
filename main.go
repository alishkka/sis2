package main

import (
	"errors"
	"fmt"
	"math"
	"time"

	"sis/task"
)

func main() {
	fmt.Println("=== Checkpoint 1: direct processing ===")
	checkpoint1()

	fmt.Println()
	fmt.Println("=== Checkpoint 2: worker pool ===")
	checkpoint2()
}

func checkpoint1() {
	svc := task.NewService(task.StatsProcessor{})

	t1, _ := svc.Create([]float64{10, 20, 30})
	t2, _ := svc.Create([]float64{5, 1, 4, 2})
	fmt.Println("created:", t1.ID, t2.ID, "state:", t1.State)

	for _, t := range []*task.Task{t1, t2} {
		if err := svc.Process(t.ID); err != nil {
			fmt.Println("error:", err)
			continue
		}
		fmt.Printf("task %d [%s]: %+v\n", t.ID, t.State, *t.Result)
	}

	if _, err := svc.Create(nil); err != nil {
		fmt.Println("error:", err)
	}

	if _, err := svc.Get(999); err != nil {
		fmt.Println("error:", err)
	}

	if err := svc.Process(t1.ID); err != nil {
		fmt.Println("error:", err)
	}

	t3, _ := svc.Create([]float64{math.MaxFloat64, math.MaxFloat64})
	if err := svc.Process(t3.ID); err != nil {
		fmt.Printf("task %d [%s]: %v\n", t3.ID, t3.State, err)
	}
}

func checkpoint2() {
	svc := task.NewService(task.StatsProcessor{})

	// 12 tasks; the last one will fail (the sum is too large)
	var ids []int
	for i := 1; i <= 11; i++ {
		t, _ := svc.Create([]float64{float64(i), float64(i * 2), float64(i * 3)})
		ids = append(ids, t.ID)
	}
	big, _ := svc.Create([]float64{math.MaxFloat64, math.MaxFloat64})
	ids = append(ids, big.ID)

	// 3 workers, room for 4 waiting tasks
	pool, err := task.NewPool(svc, 3, 4)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	// If the queue is full, wait a little and try again (the task is still Pending).
	rejected := 0
	for _, id := range ids {
		for {
			err := pool.Submit(id)
			if errors.Is(err, task.ErrQueueFull) {
				rejected++
				time.Sleep(time.Millisecond)
				continue
			}
			if err != nil {
				fmt.Printf("task %d: error: %v\n", id, err)
			}
			break
		}
	}
	fmt.Println("submit was rejected because of a full queue:", rejected, "times")

	// waits until all accepted tasks are finished and the workers have exited
	pool.Shutdown()

	if err := pool.Submit(ids[0]); err != nil {
		fmt.Println("after shutdown:", err)
	}

	for _, t := range svc.List() {
		switch {
		case t.Result != nil:
			fmt.Printf("task %d [%s]: sum=%v mean=%v\n", t.ID, t.State, t.Result.Sum, t.Result.Mean)
		case t.Error != "":
			fmt.Printf("task %d [%s]: %s\n", t.ID, t.State, t.Error)
		default:
			fmt.Printf("task %d [%s]\n", t.ID, t.State)
		}
	}
}
