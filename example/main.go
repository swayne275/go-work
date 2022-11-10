package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	wp "github.com/swayne275/go-work/workerpool"
)

type hungerTask struct {
	r          *rand.Rand
	numBurgers int
	wg         *sync.WaitGroup
}

func (j *hungerTask) Execute() error {
	defer j.wg.Done()
	log.Printf("I'm so hungry, I'm gonna eat %d cheeseburgers!\n", j.numBurgers)

	time.Sleep(time.Duration(j.numBurgers/4) * time.Second)

	if j.r.Intn(4) != 0 {
		return fmt.Errorf("%d burgers was too much :(", j.numBurgers)
	}

	log.Printf("Yay! I ate %d burgers!\n", j.numBurgers)
	return nil
}

func (j *hungerTask) OnFailure(e error) {
	log.Printf("I couldn't do it: %v\n", e)
}

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	p, err := wp.NewSimplePool(25, 0)
	if err != nil {
		log.Print("error making worker pool:", err)
		return
	}

	p.Start()
	defer p.Stop()

	wg := &sync.WaitGroup{}
	numTasks := 100
	log.Printf("dispatching %d tasks...\n", numTasks)

	wg.Add(numTasks)
	go func() {
		for i := 1; i <= numTasks; i++ {
			p.AddWork(&hungerTask{
				r:          seededRand,
				numBurgers: i,
				wg:         wg,
			})
		}
	}()

	log.Print("waiting for all tasks to complete...")
	wg.Wait()

	log.Print("all done!")
}
