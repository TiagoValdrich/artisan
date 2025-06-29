package artisan_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tiagovaldrich/artisan"
)

func TestWorkerpoolInstantiation(t *testing.T) {
	t.Run("should panic the application if no handler is provided", func(t *testing.T) {
		assert.Panics(t, func() {
			artisan.NewWorkerpool[int](2)
		}, artisan.ErrNilHandler.Error())
	})

	t.Run("should instantiate a workerpool with default error handler", func(t *testing.T) {
		artisan.NewWorkerpool(
			2,
			artisan.WithHandler(func(ctx context.Context, num int) error {
				return nil
			}),
		)
	})

	t.Run("should instantiate a workerpool with custom handler and error handler", func(t *testing.T) {
		artisan.NewWorkerpool(
			2,
			artisan.WithHandler(func(ctx context.Context, num int) error {
				return nil
			}),
			artisan.WithErrorHandler[int](func(err error) {}),
		)
	})
}

func TestWorkerpoolProcessing(t *testing.T) {
	numbersSquared := map[int]int{
		1: 1,
		2: 4,
		3: 9,
		4: 16,
		5: 25,
		6: 36,
	}

	t.Run("workerpool should be able to start, process the numbers square and shutdown gracefully when finish", func(t *testing.T) {
		var (
			errors    = make([]error, 0)
			errorLock sync.Mutex
			results   = sync.Map{}
		)

		ctx := context.Background()
		pool := artisan.NewWorkerpool(
			2,
			artisan.WithErrorHandler[int](func(err error) {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
			}),
			artisan.WithHandler(func(ctx context.Context, task int) error {
				taskResult := task * task
				results.Store(task, taskResult)

				return nil
			}),
		)
		pool.Start(ctx)

		err := pool.Process(slices.Collect(maps.Keys(numbersSquared))...)
		assert.Nil(t, err)

		pool.Wait()
		defer pool.Shutdown()

		assert.Equal(t, 0, len(errors))

		for key, value := range numbersSquared {
			resultValue, resultExists := results.Load(key)

			assert.True(t, resultExists)
			assert.Equal(t, value, resultValue)
		}
	})

	t.Run("workerpool should wait tasks finish processing before shutting down", func(t *testing.T) {
		var (
			errors    = make([]error, 0)
			errorLock sync.Mutex
			results   = sync.Map{}
		)

		ctx := context.Background()
		pool := artisan.NewWorkerpool(
			2,
			artisan.WithErrorHandler[int](func(err error) {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
			}),
			artisan.WithHandler(func(ctx context.Context, task int) error {
				time.Sleep(2 * time.Second)

				taskResult := task * task
				results.Store(task, taskResult)

				return nil
			}),
		)
		pool.Start(ctx)

		err := pool.Process(slices.Collect(maps.Keys(numbersSquared))...)
		assert.Nil(t, err)

		pool.Shutdown()

		assert.Equal(t, 0, len(errors))

		for key, value := range numbersSquared {
			resultValue, resultExists := results.Load(key)

			assert.True(t, resultExists)
			assert.Equal(t, value, resultValue)
		}
	})

	t.Run("workerpool should terminate when the context is cancelled", func(t *testing.T) {
		var (
			errors    = make([]error, 0)
			errorLock sync.Mutex
			results   = sync.Map{}
		)

		ctx := context.Background()
		ctx, cancelCtx := context.WithCancel(ctx)

		pool := artisan.NewWorkerpool(
			2,
			artisan.WithErrorHandler[int](func(err error) {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
			}),
			artisan.WithHandler(func(ctx context.Context, task int) error {
				time.Sleep(2 * time.Second)

				taskResult := task * task
				results.Store(task, taskResult)

				return nil
			}),
		)
		pool.Start(ctx)

		cancelCtx()

		err := pool.Process(slices.Collect(maps.Keys(numbersSquared))...)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("workerpool should process tasks partially on context cancellation during execution", func(t *testing.T) {
		var (
			errors             = make([]error, 0)
			errorLock          sync.Mutex
			results            = sync.Map{}
			handlerStartedChan = make(chan bool, 2)
			processWg          sync.WaitGroup
		)

		ctx := context.Background()
		ctx, cancelCtx := context.WithCancel(ctx)

		pool := artisan.NewWorkerpool(
			2,
			artisan.WithErrorHandler[int](func(err error) {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
			}),
			artisan.WithHandler(func(ctx context.Context, task int) error {
				handlerStartedChan <- true

				time.Sleep(1 * time.Second)
				taskResult := task * task
				results.Store(task, taskResult)

				return nil
			}),
		)
		pool.Start(ctx)

		// Add to wait group before starting goroutine
		processWg.Add(1)
		go func() {
			defer processWg.Done()
			_ = pool.Process(slices.Collect(maps.Keys(numbersSquared))...)
		}()

		<-handlerStartedChan
		<-handlerStartedChan

		cancelCtx()
		// Wait for Process to complete before shutdown
		processWg.Wait()
		_ = pool.Shutdown()

		resultsArr := []int{}
		results.Range(func(key, value any) bool {
			val, ok := value.(int)
			if !ok {
				panic(fmt.Errorf("unexpected value obtained"))
			}

			resultsArr = append(resultsArr, val)

			return true
		})
		resultsSize := len(resultsArr)

		assert.Equal(t, 2, resultsSize)
		assert.Equal(t, 0, len(errors))
	})

	t.Run("workerpool should recover from panics and continue processing tasks", func(t *testing.T) {
		var (
			errors    = make([]error, 0)
			results   = sync.Map{}
			errorLock sync.Mutex
			taskOrder = []int{}
			orderLock sync.Mutex
		)

		ctx := context.Background()
		pool := artisan.NewWorkerpool(
			2,
			artisan.WithErrorHandler[int](func(err error) {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
			}),
			artisan.WithHandler(func(ctx context.Context, task int) error {
				// Record the order of task processing
				orderLock.Lock()
				taskOrder = append(taskOrder, task)
				orderLock.Unlock()

				if task == 3 {
					// Simulate a panic in the handler
					panic("intentional panic for task 3")
				}

				// Small delay to ensure tasks don't complete too quickly
				time.Sleep(100 * time.Millisecond)
				results.Store(task, task*task)
				return nil
			}),
		)
		pool.Start(ctx)

		// Process tasks including the one that will panic
		err := pool.Process(1, 2, 3, 4, 5)
		assert.Nil(t, err)

		pool.Wait()
		defer pool.Shutdown()

		// Check that we got an error from the panic
		assert.Equal(t, 1, len(errors))
		assert.Contains(t, errors[0].Error(), "intentional panic for task 3")

		// Verify that other tasks were processed successfully
		successfulTasks := make(map[int]bool)
		results.Range(func(key, value any) bool {
			k := key.(int)
			v := value.(int)
			assert.Equal(t, k*k, v)
			successfulTasks[k] = true
			return true
		})

		// Should have processed all tasks except task 3 (which panicked)
		assert.Equal(t, 4, len(successfulTasks))
		assert.False(t, successfulTasks[3], "Task 3 should not have completed due to panic")

		// Verify that tasks continued to be processed after the panic
		foundLaterTask := false
		for _, task := range taskOrder {
			if task > 3 {
				foundLaterTask = true
				break
			}
		}
		assert.True(t, foundLaterTask, "Tasks after the panic should have been processed")
	})
}
