package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_NewPool(t *testing.T) {
	if _, err := NewSimplePool(0, make(chan Job)); err != ErrNoWorkers {
		t.Fatalf("expected error when creating pool with 0 workers, got: %v", err)
	}

	if _, err := NewSimplePool(5, nil); err != ErrNilJobsCh {
		t.Fatalf("expected error when creating pool with closed channel, got: %v", err)
	}

	if _, err := NewSimplePool(5, make(chan Job)); err != nil {
		t.Fatalf("expected no error creating pool, got: %v", err)
	}
}

func TestWorkerPool_MultipleStartStopDontPanic(t *testing.T) {
	p, err := NewSimplePool(5, make(chan Job))
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

type testJob struct {
	shouldErr bool
	wg        *sync.WaitGroup

	mFailure       *sync.Mutex
	failureHandled bool
}

func (t *testJob) Execute() error {
	defer t.wg.Done()

	time.Sleep(50 * time.Millisecond)

	if t.shouldErr {
		return fmt.Errorf("planned Execute() error")
	}
	return nil
}

func (t *testJob) OnFailure(e error) {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	t.failureHandled = true
}

func (t *testJob) hitFailureCase() bool {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	return t.failureHandled
}

func TestWorkerPool_Work(t *testing.T) {
	var jobs []*testJob
	wg := &sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		jobs = append(jobs, &testJob{
			shouldErr: false,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	jobsCh := make(chan Job, len(jobs))
	for _, j := range jobs {
		jobsCh <- j
	}

	p, err := NewSimplePool(5, jobsCh)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	// we'll get a timeout failure if the jobs weren't processed
	wg.Wait()

	for jobNum, job := range jobs {
		if job.hitFailureCase() {
			t.Fatalf("error function called on job %d when it shouldn't be", jobNum)
		}
	}
}

func TestWorkerPool_WorkWithErrors(t *testing.T) {
	var jobs []*testJob
	wg := &sync.WaitGroup{}

	// first 10 workers succeed
	for i := 0; i < 10; i++ {
		wg.Add(1)
		jobs = append(jobs, &testJob{
			shouldErr: false,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	// second 10 workers fail
	for i := 0; i < 10; i++ {
		wg.Add(1)
		jobs = append(jobs, &testJob{
			shouldErr: true,
			wg:        wg,

			mFailure: &sync.Mutex{},
		})
	}

	jobsCh := make(chan Job, len(jobs))
	for _, j := range jobs {
		jobsCh <- j
	}

	p, err := NewSimplePool(5, jobsCh)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	// we'll get a timeout failure if the jobs weren't processed
	wg.Wait()

	for jobNum, job := range jobs {
		if job.hitFailureCase() {
			// the first 10 jobs succeed, the second 10 fail
			if jobNum >= 10 {
				continue
			}

			t.Fatalf("error function called on job %d when it shouldn't be", jobNum)
		}
	}
}

func TestWorkerPool_StopIfJobsClosed(t *testing.T) {
	jobs := make(chan Job)
	p, err := NewSimplePool(5, jobs)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	close(jobs)
	p.Stop()
}

func TestWorkerPool_CloseJobsAfterStop(t *testing.T) {
	jobs := make(chan Job)
	p, err := NewSimplePool(5, jobs)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	p.Stop()
	close(jobs)
}

func TestWorkerPool_StartWithClosedJobs(t *testing.T) {
	jobs := make(chan Job)
	close(jobs)
	p, err := NewSimplePool(5, jobs)
	if err != nil {
		t.Fatal("error making worker pool:", err)
	}
	p.Start()

	p.Stop()
}
