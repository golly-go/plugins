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

	// Create and run the pool
	pool := NewPool(PoolConfig{
		Name: "testPool",
		MinW: 1,
		MaxW: 10,
		Handler: func(ctx *golly.Context, data interface{}) error {
			atomic.AddInt32(&jobCount, 1)
			return nil
		},
	})

	ctx := golly.NewContext(context.Background())
	go pool.Run(ctx)

	// Enqueue multiple jobs
	for i := 0; i <= jobTotal; i++ {
		err := pool.EnQueue(ctx, fmt.Sprintf("job-%d", i))
		assert.NoError(t, err)
	}

	// Wait for a reasonable time for all jobs to complete
	time.Sleep(1 * time.Second)

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

	pool := NewPool(PoolConfig{
		Name:         "testPool",
		MinW:         2,
		MaxW:         5,
		IdleTimeout:  100 * time.Millisecond,
		ReapInterval: 500 * time.Millisecond,
		Handler: func(ctx *golly.Context, data interface{}) error {
			atomic.AddInt32(&jobCount, 1)

			time.Sleep(25 * time.Millisecond) // Simulate work

			return nil
		},
	})

	ctx := golly.NewTestContext()

	go pool.Run(ctx)

	// Enqueue a few jobs
	jobTotal := 10
	for i := 0; i < jobTotal; i++ {
		pool.EnQueue(ctx, fmt.Sprintf("job-%d", i))
	}

	// Wait for all jobs to complete
	time.Sleep(1 * time.Second)

	// Verify that workers were reaped as expected
	activeWorkers := pool.activeWorkers.Load()

	assert.GreaterOrEqual(t, pool.minW, activeWorkers, "Active workers should not fall below minW")

	pool.Stop()
	pool.Wait()
}
