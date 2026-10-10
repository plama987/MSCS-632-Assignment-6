package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Task struct {
	ID      int
	Payload string
}

type Result struct {
	TaskID int
	Value  string
}

func processTask(task Task) (string, error) {
	time.Sleep(250 * time.Millisecond) // Simulate computational work.
	if strings.TrimSpace(task.Payload) == "" {
		return "", fmt.Errorf("task %d has an empty payload", task.ID)
	}
	return strings.ToUpper(task.Payload), nil
}

func worker(workerID int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started", workerID)
	for task := range tasks {
		value, err := processTask(task)
		if err != nil {
			log.Printf("Worker %d: failed to process task %d: %v", workerID, task.ID, err)
			continue
		}
		results <- Result{TaskID: task.ID, Value: value}
		log.Printf("Worker %d completed task %d", workerID, task.ID)
	}
	log.Printf("Worker %d stopped", workerID)
}

func main() {
	const workerCount = 3
	const taskCount = 10
	const outputFile = "go-results.txt"

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Println("Data processing system starting")

	tasks := make(chan Task)
	results := make(chan Result)
	var workerGroup sync.WaitGroup
	workerGroup.Add(workerCount)

	for id := 1; id <= workerCount; id++ {
		go worker(id, tasks, results, &workerGroup)
	}

	// Send tasks and close the task queue so workers can terminate.
	go func() {
		defer close(tasks)
		for id := 1; id <= taskCount; id++ {
			tasks <- Task{ID: id, Payload: fmt.Sprintf("ride-request-%d", id)}
		}
	}()

	// Only close results after all workers have finished sending.
	go func() {
		workerGroup.Wait()
		close(results)
	}()

	// Main goroutine is the sole owner of the output file and result list.
	var collected []Result
	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Could not create output file %q: %v", outputFile, err)
		// Still drain the channel to let workers finish without blocking.
		for result := range results {
			collected = append(collected, result)
		}
	} else {
		defer func() {
			if closeErr := file.Close(); closeErr != nil {
				log.Printf("Error closing output file: %v", closeErr)
			}
		}()
		for result := range results {
			collected = append(collected, result)
			if _, writeErr := fmt.Fprintf(file, "Task %d -> %s\n", result.TaskID, result.Value); writeErr != nil {
				log.Printf("Could not write task %d: %v", result.TaskID, writeErr)
			}
		}
	}

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].TaskID < collected[j].TaskID
	})
	fmt.Printf("\nProcessed results (%d):\n", len(collected))
	for _, result := range collected {
		fmt.Printf("Task %d -> %s\n", result.TaskID, result.Value)
	}
	fmt.Printf("Expected tasks: %d; completed tasks: %d\n", taskCount, len(collected))
	fmt.Printf("Output file: %s\n", outputFile)
	log.Println("Data processing system finished")
}
