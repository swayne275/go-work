package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	wp "github.com/swayne275/go-work/workerpool"
)

type hungerJob struct {
	r          *rand.Rand
	numBurgers int
	wg         *sync.WaitGroup
}

func (j *hungerJob) Execute() error {
	defer j.wg.Done()
	log.Printf("I'm so hungry, I'm gonna eat %d cheeseburgers!\n", j.numBurgers)

	time.Sleep(time.Duration(j.numBurgers/4) * time.Second)

	if j.r.Intn(4) != 0 {
		return fmt.Errorf("%d burgers was too much :(", j.numBurgers)
	}

	log.Printf("Yay! I ate %d burgers!\n", j.numBurgers)
	return nil
}

func (j *hungerJob) OnFailure(e error) {
	log.Printf("I couldn't do it: %v\n", e)
}

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	jobs := make(chan wp.Job)
	p, err := wp.NewSimplePool(25, jobs)
	if err != nil {
		log.Print("error making worker pool:", err)
		return
	}

	p.Start()
	defer p.Stop()

	wg := &sync.WaitGroup{}
	numJobs := 100
	log.Printf("dispatching %d jobs...\n", numJobs)

	go func() {
		for i := 1; i <= numJobs; i++ {
			wg.Add(1)
			jobs <- &hungerJob{
				r:          seededRand,
				numBurgers: i,
				wg:         wg,
			}
		}
	}()

	log.Print("waiting for all jobs to complete...")
	wg.Wait()

	log.Print("all done!")
}
