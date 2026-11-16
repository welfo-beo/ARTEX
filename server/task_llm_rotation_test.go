package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
)

func TestLLMProfilePinRoundTrip(t *testing.T) {
	ctx := context.Background()
	if _, ok := llmProfilePinFrom(ctx); ok {
		t.Fatal("empty context must carry no pin")
	}
	pinned := withLLMProfilePin(ctx, 42)
	got, ok := llmProfilePinFrom(pinned)
	if !ok || got != 42 {
		t.Fatalf("pin roundtrip got=%d ok=%v", got, ok)
	}
}

func TestNextRotatedProfileRoundRobin(t *testing.T) {
	tk := &Task{LLMProfileIDs: []int64{3, 1, 2}}
	want := []int64{3, 1, 2, 3, 1}
	for i, w := range want {
		got, ok := tk.nextRotatedProfile()
		if !ok || got != w {
			t.Fatalf("rotation %d: got=%d ok=%v want=%d", i, got, ok, w)
		}
	}
	if _, ok := (&Task{}).nextRotatedProfile(); ok {
		t.Fatal("empty chain must not rotate")
	}
}

func rotationRuntimeForTest(t *testing.T, providers ...llm.Provider) (*taskLLMRuntime, []int64) {
	t.Helper()
	skipWithoutDSN(t)
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	ids := make([]int64, len(providers))
	s := &Server{m: m, provByProfile: map[int64]*provEntry{}}
	for i, provider := range providers {
		ids[i], err = m.pg.SaveProfile(&db.LLMProfile{
			Name:   fmt.Sprintf("pin-%d-%d", time.Now().UnixNano(), i),
			Format: "openai", Model: "test", APIKey: "test-key",
		})
		if err != nil {
			t.Fatal(err)
		}
		id := ids[i]
		t.Cleanup(func() { _ = m.pg.DeleteProfile(id) })
		s.provByProfile[id] = &provEntry{prov: provider}
	}
	task, err := m.CreateTaskWithOptions("pinned model", "test failover", db.TaskCreateOptions{LLMProfileIDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = m.DeleteTask(task.ID, DeleteTaskOptions{}) })
	return &taskLLMRuntime{s: s, taskID: task.ID, agentKey: "unbound-pin-test-role"}, ids
}

func TestTaskLLMPinnedProfileFailsOver(t *testing.T) {
	for _, mode := range []string{"stream", "complete"} {
		for _, pinnedIndex := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/pin-%d", mode, pinnedIndex), func(t *testing.T) {
				bad := &scriptedLLMProvider{err: errors.New("insufficient_quota")}
				good := &scriptedLLMProvider{events: []llm.StreamEvent{{Type: llm.SETextDelta, Text: "backup reply"}}}
				providers := []llm.Provider{good, bad}
				if pinnedIndex == 0 {
					providers = []llm.Provider{bad, good}
				}
				runtime, ids := rotationRuntimeForTest(t, providers...)
				hooks := runtime.streamHooks()
				hooks.transition = nil
				current := hooks.current
				selections := 0
				hooks.current = func(ctx context.Context) (taskLLMSelection, error) {
					selections++
					if selections > 3 {
						return taskLLMSelection{}, errors.New("exhausted pin was repeatedly selected")
					}
					return current(ctx)
				}
				ctx := withLLMProfilePin(context.Background(), ids[pinnedIndex])
				if mode == "stream" {
					events, err := collectTaskLLMStream(streamTaskLLM(ctx, runtime.taskID, llm.CompletionRequest{}, hooks))
					if err != nil || len(events) != 1 || events[0].Text != "backup reply" {
						t.Fatalf("stream events=%+v err=%v", events, err)
					}
				} else if _, _, _, err := completeTaskLLM(ctx, runtime.taskID, llm.CompletionRequest{}, hooks); err != nil {
					t.Fatal(err)
				}
				if bad.calls != 1 || good.calls != 1 {
					t.Fatalf("exhausted calls=%d backup calls=%d, want one each", bad.calls, good.calls)
				}
				selected, err := runtime.currentFor(ctx)
				if err != nil || selected.profileID != ids[1-pinnedIndex] {
					t.Fatalf("later call selected=%d err=%v", selected.profileID, err)
				}
			})
		}
	}
}

func TestTaskLLMProfilePinRequiresReadyEntry(t *testing.T) {
	runtime, ids := rotationRuntimeForTest(t, &scriptedLLMProvider{}, &scriptedLLMProvider{})
	for _, tc := range []struct{ pin, want int64 }{
		{ids[1], ids[1]}, // a ready nonactive pin is honored
		{-1, ids[0]},     // a profile outside the chain cannot override its head
	} {
		selected, err := runtime.currentFor(withLLMProfilePin(context.Background(), tc.pin))
		if err != nil || selected.profileID != tc.want {
			t.Fatalf("pin=%d selected=%d want=%d err=%v", tc.pin, selected.profileID, tc.want, err)
		}
	}
	taskID, _ := parseTaskID(runtime.taskID)
	for _, id := range ids {
		if _, err := runtime.s.m.pg.MarkTaskLLMProfileQuotaExhausted(taskID, id, "insufficient_quota"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runtime.currentFor(withLLMProfilePin(context.Background(), ids[0])); !isTaskLLMChainExhausted(err) {
		t.Fatalf("pin revived exhausted chain: %v", err)
	}
}
