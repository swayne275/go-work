package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_NewPool(t *testing.T) {
	if _, err := NewSimplePool(0, 0); err != ErrNoWorkers {
		t.Fatalf("expected error when creating pool with 0 workers, got: %v", err)
	}
	if _, err := NewSimplePool(-1, 0); err != ErrNoWorkers {
		t.Fatalf("expected error when creating pool with -1 workers, got: %v", err)
	}
	if _, err := NewSimplePool(1, -1); err != ErrNegativeChannelSize {
		t.Fatalf("expected error when creating pool with -1 channel size, got: %v", err)
	}

	if _, err := NewSimplePool(5, 0); err != nil {
		t.Fatalf("expected no error creating pool, got: %v", err)
	}
}

func TestWorkerPool_MultipleStartStopDontPanic(t *testing.T) {
	p, err := NewSimplePool(5, 0)
	if err != nil {
		t.Fatal("error creating pool:", err)
	}

	// We're just checking to make sure multiple calls to start or stop
	// don't cause a panic
	p.Start()
	p.Start()

	p.Stop()
	p.Stop()
}

type testTask struct {
	executeFunc func() error

	shouldErr bool
	wg        *sync.WaitGroup

	mFailure       *sync.Mutex
	failureHandled bool
}

func newTestTask(executeFunc func() error, shouldErr bool, wg *sync.WaitGroup) *testTask {
	return &testTask{
		executeFunc: executeFunc,
		shouldErr:   shouldErr,
		wg:          wg,
		mFailure:    &sync.Mutex{},
	}
}

func (t *testTask) Execute() error {
	if t.wg != nil {
		defer t.wg.Done()
	}

	if t.executeFunc != nil {
		return t.executeFunc()
	}

	// if no function provided, just wait and error if told to do so
	time.Sleep(50 * time.Millisecond)
	if t.shouldErr {
		return fmt.Errorf("planned Execute() error")
	}
	return nil
}

func (t *testTask) OnFailure(e error) {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	t.failureHandled = true
}

func (t *testTask) hitFailureCase() bool {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	return t.failureHandled
}

func TestWorkerPool_Work(t *testing.T) {
	var tasks []*testTask
	wg := &sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		tasks = append(tasks, newTestTask(nil, false, wg))
	}

	p, err := NewSimplePool(5, len(tasks))
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	for _, j := range tasks {
		p.AddWork(j)
	}

	// we'll get a timeout failure if the tasks weren't processed
	wg.Wait()

	for taskNum, task := range tasks {
		if task.hitFailureCase() {
			t.Fatalf("error function called on task %d when it shouldn't be", taskNum)
		}
	}
}

func TestWorkerPool_WorkWithErrors(t *testing.T) {
	var tasks []*testTask
	wg := &sync.WaitGroup{}

	// first 10 workers succeed
	for i := 0; i < 10; i++ {
		wg.Add(1)
		tasks = append(tasks, newTestTask(nil, false, wg))
	}

	// second 10 workers fail
	for i := 0; i < 10; i++ {
		wg.Add(1)
		tasks = append(tasks, newTestTask(nil, true, wg))
	}

	p, err := NewSimplePool(5, len(tasks))
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	for _, j := range tasks {
		p.AddWork(j)
	}

	// we'll get a timeout failure if the tasks weren't processed
	wg.Wait()

	for taskNum, task := range tasks {
		if task.hitFailureCase() {
			// the first 10 tasks succeed, the second 10 fail
			if taskNum >= 10 {
				continue
			}

			t.Fatalf("error function called on task %d when it shouldn't be", taskNum)
		}
	}
}

func TestWorkerPool_BlockedAddWorkReleaseAfterStop(t *testing.T) {
	p, err := NewSimplePool(1, 0)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}

	p.Start()

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		// the first should start processing right away, the second two should hang
		wg.Add(1)
		go func() {
			p.AddWork(newTestTask(func() error {
				time.Sleep(20 * time.Second)
				return nil
			}, false, nil))
			wg.Done()
		}()
	}

	done := make(chan struct{})
	p.Stop()
	go func() {
		// wait on our AddWork calls to complete, then signal on the done channel
		wg.Wait()
		done <- struct{}{}
	}()

	// wait until either we hit our timeout, or we're told the AddWork calls completed
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("failed because still hanging on AddWork")
	case <-done:
		// this is the success case
	}
}
