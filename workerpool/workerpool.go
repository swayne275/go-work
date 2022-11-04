package workerpool

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Pool interface {
	Start()
	Stop()
}

type Job interface {
	// Execute performs the work
	Execute() error
	// OnFailure handles any error returned from Execute()
	OnFailure(error)
}

type SimplePool struct {
	numWorkers int
	jobs       chan Job

	// ensure the pool can only be started once
	start sync.Once
	// ensure the pool can only be stopped once
	stop sync.Once

	// close to signal the workers to stop working
	quit chan struct{}
}

var _ Pool = (*SimplePool)(nil)

var ErrNoWorkers = fmt.Errorf("attempting to create worker pool with zero workers")
var ErrNilJobsCh = fmt.Errorf("attempting to create worker pool with nil jobs channel")

func NewSimplePool(numWorkers int, jobs chan Job) (Pool, error) {
	if numWorkers == 0 {
		return nil, ErrNoWorkers
	}
	if jobs == nil {
		return nil, ErrNilJobsCh
	}

	return &SimplePool{
		numWorkers: numWorkers,
		jobs:       jobs,

		start: sync.Once{},
		stop:  sync.Once{},

		quit: make(chan struct{}),
	}, nil
}

func (p *SimplePool) Start() {
	p.start.Do(func() {
		log.Print("starting simple worker pool")
		p.startWorkers()
	})
}

func (p *SimplePool) Stop() {
	p.stop.Do(func() {
		log.Print("stopping simple worker pool")
		close(p.quit)
	})
}

func (p *SimplePool) startWorkers() {
	for i := 0; i < p.numWorkers; i++ {
		go func(workerNum int) {
			log.Printf("starting worker %d", workerNum)

			ticker := time.NewTicker(10 * time.Second)
			for {
				ticker.Reset(10 * time.Second)

				select {
				case <-p.quit:
					log.Printf("stopping worker %d with quit channel\n", workerNum)
					return
				case job, ok := <-p.jobs:
					if !ok {
						log.Printf("stopping worker %d with closed jobs channel\n", workerNum)
						return
					}

					if err := job.Execute(); err != nil {
						job.OnFailure(err)
					}
				case t := <-ticker.C:
					// this is mostly for developers to catch that the worker is waiting around idle
					log.Printf("worker %d idle at %v\n", workerNum, t)
				}
			}
		}(i)
	}
}
