package workerpool

import (
	"fmt"
	"log"
	"sync"
)

type Pool interface {
	Start()
	Stop()
}

type Task interface {
	// Execute performs the work
	Execute() error
	// OnFailure handles any error returned from Execute()
	OnFailure(error)
}

type SimplePool struct {
	numWorkers int
	tasks      chan Task

	// ensure the pool can only be started once
	start sync.Once
	// ensure the pool can only be stopped once
	stop sync.Once

	// close to signal the workers to stop working
	quit chan struct{}
}

var _ Pool = (*SimplePool)(nil)

var ErrNoWorkers = fmt.Errorf("attempting to create worker pool with zero workers")
var ErrNilTasksCh = fmt.Errorf("attempting to create worker pool with nil tasks channel")

func NewSimplePool(numWorkers int, tasks chan Task) (Pool, error) {
	if numWorkers == 0 {
		return nil, ErrNoWorkers
	}
	if tasks == nil {
		return nil, ErrNilTasksCh
	}

	return &SimplePool{
		numWorkers: numWorkers,
		tasks:      tasks,

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

			for {
				select {
				case <-p.quit:
					log.Printf("stopping worker %d with quit channel\n", workerNum)
					return
				case task, ok := <-p.tasks:
					if !ok {
						log.Printf("stopping worker %d with closed tasks channel\n", workerNum)
						return
					}

					if err := task.Execute(); err != nil {
						task.OnFailure(err)
					}
				}
			}
		}(i)
	}
}
