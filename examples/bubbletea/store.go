package main

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// task is one immutable record after it has been installed in the shared query cache.
type task struct {
	// id identifies the record independently of its position in the task board.
	id int
	// title describes the work shown in the list and selected detail.
	title string
	// done records whether the work has been completed.
	done bool
}

// taskStore is the local backend; it never shares its mutable slice with cached results.
type taskStore struct {
	// mu serializes backend reads, mutations, and failure injection.
	mu sync.Mutex
	// tasks owns the mutable records; list returns a detached copy.
	tasks []task
	// failNext causes exactly one backend read to fail, then permits recovery.
	failNext bool
}

// newTaskStore supplies a small, stable dataset for the offline demonstration.
func newTaskStore() *taskStore {
	return &taskStore{tasks: []task{
		{id: 1, title: "Share queries across components"},
		{id: 2, title: "Keep previous data during refresh"},
		{id: 3, title: "Recover from a failed refresh", done: true},
	}}
}

// list returns a detached snapshot or consumes the next injected backend failure.
func (s *taskStore) list() ([]task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext {
		s.failNext = false
		return nil, errors.New("simulated backend failure; press r to recover")
	}
	return slices.Clone(s.tasks), nil
}

// toggle changes backend state by identity without modifying any previously cached result.
func (s *taskStore) toggle(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.tasks {
		if s.tasks[i].id == id {
			s.tasks[i].done = !s.tasks[i].done
			return nil
		}
	}
	return fmt.Errorf("task %d does not exist", id)
}

// fail makes the next load fail so the user can inspect retained data and retry explicitly.
func (s *taskStore) fail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = true
}
