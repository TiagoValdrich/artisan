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
	internalCtx  context.Context
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
	w.internalCtx, w.shutdownCtx = context.WithCancel(ctx)

	for i := 0; i < w.size; i++ {
		go w.spawnWorker(w.internalCtx)
	}
}

func (w *Workerpool[Task]) Process(tasks ...Task) error {
	if w.internalCtx == nil || w.shutdownCtx == nil {
		return ErrWorkerpoolNotStarted
	}

	for _, task := range tasks {
		w.wg.Add(1)

		select {
		case <-w.internalCtx.Done():
			w.wg.Done()
			return w.internalCtx.Err()
		case w.tasksChannel <- task:
			continue
		}
	}

	return nil
}

func (w *Workerpool[Task]) Wait() error {
	doneChan := make(chan struct{})

	func() {
		w.wg.Wait()
		close(doneChan)
	}()

	select {
	case <-w.internalCtx.Done():
		return w.internalCtx.Err()
	case <-doneChan:
		return nil
	}
}

func (w *Workerpool[Task]) Shutdown() error {
	close(w.tasksChannel)

	if err := w.Wait(); err != nil {
		return err
	}

	if w.shutdownCtx != nil {
		w.shutdownCtx()
	}

	return nil
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

			w.processTask(ctx, task)
		}
	}
}

func (w *Workerpool[Task]) processTask(ctx context.Context, task Task) {
	defer w.wg.Done()
	defer w.recover()

	err := w.handler(ctx, task)
	if err != nil {
		w.errorHandler(err)
	}
}

func (w *Workerpool[Task]) recover() {
	if r := recover(); r != nil {
		err := fmt.Errorf("artisan: panic recovered in worker while processing task: %v", r)

		w.errorHandler(err)
	}
}
