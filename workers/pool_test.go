package workers

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

func TestEnQueueMultipleJobs(t *testing.T) {
	var jobCount int32
	jobTotal := 50

	// Define a handler function that increments jobCount upon completion
	handler := func(ctx golly.Context, data interface{}) error {
		atomic.AddInt32(&jobCount, 1)
		return nil
	}

	// Create and run the pool
	pool := NewPool("testPool", 1, 10, handler)
	ctx := golly.NewContext(context.Background())
	go pool.Run(ctx)

	// Enqueue multiple jobs
	for i := 0; i < jobTotal; i++ {
		err := pool.EnQueue(ctx, fmt.Sprintf("job-%d", i))
		assert.NoError(t, err)
	}

	// Wait for a reasonable time for all jobs to complete
	time.Sleep(100 * time.Millisecond)

	// Stop the pool
	pool.Stop()
	pool.Wait()

	// Check if all jobs were processed
	processedJobs := atomic.LoadInt32(&jobCount)
	assert.Equal(t, int32(jobTotal), processedJobs, "Not all jobs were processed")
}

func TestReapFunctionality(t *testing.T) {
	// Variables to track job completion
	var jobCount int32

	// Worker handler that simulates work
	handler := func(ctx golly.Context, data interface{}) error {
		time.Sleep(25 * time.Millisecond) // Simulate work
		atomic.AddInt32(&jobCount, 1)
		return nil
	}

	// Create and run the pool with more workers than minW
	minW, maxW := int32(2), int32(5)
	pool := NewPool("testPool", minW, maxW, handler)

	ctx := golly.NewContext(context.Background())
	go pool.Run(ctx)

	pool.idleTimeout = 100 * time.Millisecond

	// Enqueue a few jobs
	jobTotal := 10
	for i := 0; i < jobTotal; i++ {
		pool.EnQueue(ctx, fmt.Sprintf("job-%d", i))
	}

	// Wait for all jobs to complete
	time.Sleep(1 * time.Second)

	// Verify that workers were reaped as expected
	activeWorkers := pool.activeWorkers.Load()
	assert.LessOrEqual(t, activeWorkers, maxW, "Active workers should not exceed maxW")
	assert.GreaterOrEqual(t, activeWorkers, minW, "Active workers should not fall below minW")

	pool.Stop()
	pool.Wait()
}
