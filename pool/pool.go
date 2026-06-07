package main

import (
	"fmt"
	"sync"
	"time"
)

//Create Task
	type Task struct{
		ID int
	}
	//Process Task
	func (t *Task) Process(){
		fmt.Printf("The task id is %d",t.ID)
		time.Sleep(2 * time.Second)

	}
	//Workpool creation
	type WorkPool struct{
		Tasks 			[]Task
		concurrency 	int
		Taskschan 		chan Task
		wg 				sync.WaitGroup
	}
	//Workpool execution
	func (wp *WorkPool) worker(){
		for task := range wp.Taskschan{
			task.Process()
			wp.wg.Done()
		}
	}

	func (wp *WorkPool) Run(){
		//Intialise the task channel
		wp.Taskschan = make(chan Task,len(wp.Tasks))

		//start workers
		for i := 0;i<wp.concurrency;i++{
			go wp.worker()
		}
		//Send task to the task channel
		wp.wg.Add(len(wp.Tasks))
		for _,task := range wp.Tasks{
			wp.Taskschan <- task
		}
		close(wp.Taskschan)

		//Wait all the tasks to be finished
		wp.wg.Wait()
	}