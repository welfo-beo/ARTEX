package server

import (
	"context"
	"sync"
	"testing"

	"github.com/Autumn-27/artex/agent"
)

func TestIntentContextKeepsModelPinsLocal(t *testing.T) {
	for _, plannerFirst := range []bool{true, false} {
		name := "worker-first"
		if plannerFirst {
			name = "planner-first"
		}
		t.Run(name, func(t *testing.T) {
			e := NewEngine(nil)
			task := &Task{ID: "rotating", LLMRotate: true, LLMProfileIDs: []int64{11, 22}}
			parent := context.Background()
			if plannerFirst {
				e.execContextFor(parent, task.ID)
			}
			for _, want := range []int64{11, 22, 11} {
				worker := e.intentContextFor(parent, task)
				if got, ok := llmProfilePinFrom(worker); !ok || got != want {
					t.Fatalf("worker pin=%d present=%v, want %d", got, ok, want)
				}
				planner := e.execContextFor(parent, task.ID)
				if got, ok := llmProfilePinFrom(planner); ok {
					t.Fatalf("planner inherited worker pin %d", got)
				}
			}
		})
	}
}

func TestConcurrentIntentContextsKeepDistinctPins(t *testing.T) {
	e := NewEngine(nil)
	task := &Task{ID: "parallel", LLMRotate: true}
	const workers = 32
	for i := range workers {
		task.LLMProfileIDs = append(task.LLMProfileIDs, int64(i+1))
	}
	contexts := make(chan context.Context, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { contexts <- e.intentContextFor(context.Background(), task) })
	}
	wg.Wait()
	close(contexts)
	seen := make(map[int64]bool)
	for ctx := range contexts {
		pin, ok := llmProfilePinFrom(ctx)
		if !ok || seen[pin] {
			t.Fatalf("worker pin=%d present=%v duplicated=%v", pin, ok, seen[pin])
		}
		seen[pin] = true
	}
	if _, ok := llmProfilePinFrom(e.execContextFor(context.Background(), task.ID)); ok {
		t.Fatal("parallel workers polluted the cached task context")
	}
}

func TestIntentContextPreservesPauseResume(t *testing.T) {
	e := NewEngine(nil)
	task := &Task{ID: "pause-rotation", LLMRotate: true, LLMProfileIDs: []int64{11, 22}, notify: make(chan struct{}, 1)}
	first := e.intentContextFor(context.Background(), task)
	second := e.intentContextFor(context.Background(), task)
	e.Pause(task.ID, agent.AbortPausedByUser)
	for _, ctx := range []context.Context{first, second} {
		if code, _, _, ok := agent.AbortReason(ctx); !ok || code != "paused_by_user" {
			t.Fatalf("pause cause=%q present=%v", code, ok)
		}
	}
	paused := e.intentContextFor(context.Background(), task)
	if paused.Err() == nil {
		t.Fatal("paused worker received a live context")
	}
	e.Resume(task)
	fresh := e.intentContextFor(context.Background(), task)
	if fresh.Err() != nil {
		t.Fatalf("resumed worker is cancelled: %v", fresh.Err())
	}
	if pin, ok := llmProfilePinFrom(fresh); !ok || pin != 22 {
		t.Fatalf("resumed worker pin=%d present=%v, want 22", pin, ok)
	}
	if first.Err() == nil || second.Err() == nil {
		t.Fatal("resume revived old worker contexts")
	}
}

func TestIntentContextsFollowParentCancellation(t *testing.T) {
	e := NewEngine(nil)
	task := &Task{ID: "parent", LLMRotate: true, LLMProfileIDs: []int64{11, 22}}
	parent, cancel := context.WithCancel(context.Background())
	first := e.intentContextFor(parent, task)
	second := e.intentContextFor(parent, task)
	cancel()
	if first.Err() == nil || second.Err() == nil {
		t.Fatal("worker contexts lost parent cancellation")
	}
}

func TestIntentContextWithoutRotationHasNoPin(t *testing.T) {
	for _, task := range []*Task{
		{ID: "disabled", LLMProfileIDs: []int64{11}},
		{ID: "empty", LLMRotate: true},
	} {
		e := NewEngine(nil)
		if pin, ok := llmProfilePinFrom(e.intentContextFor(context.Background(), task)); ok {
			t.Fatalf("task %s unexpectedly pinned %d", task.ID, pin)
		}
	}
}
