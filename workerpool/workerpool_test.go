package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_NewPool(t *testing.T) {
	if _, err := NewSimplePool(0, make(chan Task)); err != ErrNoWorkers {
		t.Fatalf("expected error when creating pool with 0 workers, got: %v", err)
	}

	if _, err := NewSimplePool(5, nil); err != ErrNilTasksCh {
		t.Fatalf("expected error when creating pool with closed channel, got: %v", err)
	}

	if _, err := NewSimplePool(5, make(chan Task)); err != nil {
		t.Fatalf("expected no error creating pool, got: %v", err)
	}
}

func TestWorkerPool_MultipleStartStopDontPanic(t *testing.T) {
	p, err := NewSimplePool(5, make(chan Task))
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
	shouldErr bool
	wg        *sync.WaitGroup

	mFailure       *sync.Mutex
	failureHandled bool
}

func (t *testTask) Execute() error {
	defer t.wg.Done()

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
		tasks = append(tasks, &testTask{
			shouldErr: false,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	tasksCh := make(chan Task, len(tasks))
	for _, j := range tasks {
		tasksCh <- j
	}

	p, err := NewSimplePool(5, tasksCh)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

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
		tasks = append(tasks, &testTask{
			shouldErr: false,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	// second 10 workers fail
	for i := 0; i < 10; i++ {
		wg.Add(1)
		tasks = append(tasks, &testTask{
			shouldErr: true,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	tasksCh := make(chan Task, len(tasks))
	for _, j := range tasks {
		tasksCh <- j
	}

	p, err := NewSimplePool(5, tasksCh)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

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

func TestWorkerPool_StopIfTasksClosed(t *testing.T) {
	tasks := make(chan Task)
	p, err := NewSimplePool(5, tasks)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	close(tasks)
	p.Stop()
}

func TestWorkerPool_CloseTasksAfterStop(t *testing.T) {
	tasks := make(chan Task)
	p, err := NewSimplePool(5, tasks)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	p.Stop()
	close(tasks)
}

func TestWorkerPool_StartWithClosedTasks(t *testing.T) {
	tasks := make(chan Task)
	close(tasks)
	p, err := NewSimplePool(5, tasks)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	p.Stop()
}
