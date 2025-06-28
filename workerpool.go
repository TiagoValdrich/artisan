package artisan

import (
	"context"
	"fmt"
	"sync"
)

type WorkerpoolHandler[Task any] func(ctx context.Context, task Task) error
type WorkerpoolErrorHandler func(err error)
type WorkerpoolOption[Task any] func(*Workerpool[Task])

type Workerpool[Task any] struct {
	size         int
	handler      WorkerpoolHandler[Task]
	errorHandler WorkerpoolErrorHandler
	tasksChannel chan Task
	shutdownCtx  context.CancelFunc
	wg           sync.WaitGroup
}

func NewWorkerpool[Task any](size int, options ...WorkerpoolOption[Task]) *Workerpool[Task] {
	workerpool := &Workerpool[Task]{
		size:         size,
		tasksChannel: make(chan Task),
	}

	for _, opt := range options {
		opt(workerpool)
	}

	workerpool.validateConfig()

	return workerpool
}

func WithHandler[Task any](handler WorkerpoolHandler[Task]) WorkerpoolOption[Task] {
	return func(w *Workerpool[Task]) {
		w.handler = handler
	}
}

func WithErrorHandler[Task any](errorHandler WorkerpoolErrorHandler) WorkerpoolOption[Task] {
	return func(w *Workerpool[Task]) {
		w.errorHandler = errorHandler
	}
}

func (w *Workerpool[Task]) validateConfig() {
	if w.handler == nil {
		panic(ErrNilHandler)
	}

	if w.errorHandler == nil {
		w.setupDefaultErrorHandler()
	}
}

func (w *Workerpool[Task]) setupDefaultErrorHandler() {
	w.errorHandler = func(err error) {
		fmt.Printf("an error has occurred: %v", err)
	}
}

func (w *Workerpool[Task]) Start(ctx context.Context) {
	ctx, w.shutdownCtx = context.WithCancel(ctx)

	for i := 0; i < w.size; i++ {
		go w.spawnWorker(ctx)
	}
}

func (w *Workerpool[Task]) Process(tasks ...Task) {
	for _, task := range tasks {
		w.wg.Add(1)
		w.tasksChannel <- task
	}
}

func (w *Workerpool[Task]) Wait() {
	w.wg.Wait()
}

func (w *Workerpool[Task]) Shutdown() {
	close(w.tasksChannel)

	w.wg.Wait()

	if w.shutdownCtx != nil {
		w.shutdownCtx()
	}
}

func (w *Workerpool[Task]) GetAproximatedTasksWaiting() int {
	return len(w.tasksChannel)
}

func (w *Workerpool[Task]) spawnWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-w.tasksChannel:
			if !ok {
				return
			}

			err := w.handler(ctx, task)
			if err != nil {
				w.errorHandler(err)
			}

			w.wg.Done()
		}
	}
}
