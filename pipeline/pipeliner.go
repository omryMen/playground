package pipeline

import (
	"context"
	"sync"
)

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Panic(args ...interface{})
	Fatal(args ...interface{})
}

type Item struct {
	Data   any
	Key    string
	Offset int
}

type Source interface {
	List(ctx context.Context, offset, chunkSize int,
		sinkFn func(context.Context, []Item) error, close func())
}

type Processor func(ctx context.Context, items []Item) []Item

type Destination interface {
	Send(ctx context.Context, data chan []Item)
}

type Config struct {
	Concurrency int
	ChunkSize   int
}

type Pipeliner struct {
	config      Config
	source      Source
	processor   Processor // serial processing in order
	destination Destination
}

func Initialize(config Config, source Source,
	processor Processor, destination Destination) Pipeliner {

	return Pipeliner{
		config:      config,
		source:      source,
		processor:   processor,
		destination: destination,
	}
}

func stopIfErr(ctx context.Context, errs chan error, cancel context.CancelFunc, logger Logger) func(err error) bool {
	return func(err error) bool {
		if err == nil {
			return false
		}

		if logger != nil {
			logger.Error("pipeline error: ", err)
		}

		select {
		case <-ctx.Done():
			return false
		case errs <- err:
			cancel()
		}

		return true
	}
}

func (p *Pipeliner) Run(ctx context.Context) error {
	listChan := make(chan []Item)
	sinkFn := func(ctx context.Context, items []Item) error {
		listChan <- items
		return nil
	}
	closeFn := func() {
		close(listChan)
	}
	go p.source.List(ctx, 0, p.config.ChunkSize, sinkFn, closeFn)

	processedChan := parallel(ctx, p.config.Concurrency, listChan, p.processor)
	p.destination.Send(ctx, processedChan)

	return nil
}

func parallel(ctx context.Context, numberOfPipes int, in chan []Item,
	fn func(ctx context.Context, items []Item) []Item) chan []Item {
	processedChan := make(chan []Item)
	go func() {
		defer close(processedChan)
		wg := &sync.WaitGroup{}
		wg.Add(numberOfPipes)
		for i := 0; i < numberOfPipes; i++ {
			go func(index int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case v, ok := <-in:
						if !ok {
							return // channel was closed
						}

						processed := fn(ctx, v)
						if len(processed) > 0 {
							processedChan <- processed
						}
					}
				}
			}(i)
		}
		wg.Wait()
	}()
	return processedChan
}
