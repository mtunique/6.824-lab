package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	var wg sync.WaitGroup

	for taskIndex := 0; taskIndex < ntasks; taskIndex++ {
		fileName := ""
		if phase == mapPhase {
			fileName = mapFiles[taskIndex]
		}

		wg.Add(1)
		go func(taskIndex int) {
			for {
				workerAddress := <- registerChan
				debug("Start worker %s task %d %s\n", workerAddress, taskIndex, fileName)

				if call(workerAddress,
					"Worker.DoTask",
					DoTaskArgs{
						jobName,
						fileName,
						phase,
						taskIndex,
						n_other,
					},
					nil) {
					debug("Done worker %s task %d\n", workerAddress, taskIndex)
					wg.Done()
					select {
					case registerChan <- workerAddress:
						debug("Add address %s\n", workerAddress)
					}
					break
				}
			}
		}(taskIndex)
	}

	debug("Wait for done\n")
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
