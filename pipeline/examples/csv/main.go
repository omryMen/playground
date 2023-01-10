package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/omryMen/playground/pipeline"
)

func main() {
	source := fileReader{fileName: "source.csv"}
	processor := func(ctx context.Context, in []pipeline.Item) []pipeline.Item {
		res := make([]pipeline.Item, 0, len(in))
		for i, p := range in {
			tmp := p.Data.(item)
			tmp.Price += i
			p.Data = tmp
			res = append(res, p)
		}
		return res
	}
	dest := fileWriter{filename: "output.csv"}
	pipe := pipeline.Initialize(pipeline.Config{Concurrency: 2, ChunkSize: 1},
		&source, processor, &dest)
	fmt.Println(pipe.Run(context.Background()))

	c := context.Background()
	c.Deadline()
	maxConcurrent := 5
	ids := []string{"a"}

	wg := sync.WaitGroup{}
	sem := make(chan struct{}, maxConcurrent)
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			fmt.Println(id) // do something
		}(id)
	}
	wg.Wait()

}

type item struct {
	ID, Desc      string
	Amount, Price int
}

type fileReader struct {
	fileName string
}

func (f *fileReader) List(ctx context.Context, offset, chunkSize int,
	sinkFn func(context.Context, []pipeline.Item) error, close func()) {
	defer close()
	// for the example, ignoring offset and chunk size
	reader, err := os.Open(f.fileName)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	csvReader := csv.NewReader(reader)
	i := 0
	for {
		line, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else if errors.Is(err, csv.ErrFieldCount) {
				fmt.Println(err)
				continue
			} else {
				panic(err)
			}
		}
		if i == 0 {
			i++ // skip headers
			continue
		}
		tmpItem := lineToItem(line)
		items := []pipeline.Item{{
			Data:   tmpItem,
			Key:    tmpItem.ID,
			Offset: i,
		}}
		err = sinkFn(ctx, items)
		if err != nil {
			panic(err)
		}
		i++
	}
}

func lineToItem(line []string) item {
	amount, _ := strconv.Atoi(line[2])
	price, _ := strconv.Atoi(line[3])
	return item{
		ID:     line[0],
		Desc:   line[1],
		Amount: amount,
		Price:  price,
	}
}

func itemToLine(i item) []string {
	return []string{i.ID, i.Desc,
		strconv.Itoa(i.Amount), strconv.Itoa(i.Price)}
}

type fileWriter struct {
	filename string
}

func (f *fileWriter) Send(ctx context.Context, data chan []pipeline.Item) {
	file, err := os.Create(f.filename)
	if err != nil {
		panic(err)
	}

	writer := csv.NewWriter(file)
	err = writer.Write([]string{"id", "description", "amount", "price"})
	if err != nil {
		panic(err)
	}
	defer writer.Flush()

	for items := range data {
		select {
		case <-ctx.Done():
			return
		default:
			for _, p := range items {
				err = writer.Write(itemToLine(p.Data.(item)))
				if err != nil {
					panic(err)
				}

			}
		}

	}
}
