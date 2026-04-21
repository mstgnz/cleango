package cleaner

import (
	"context"
	"runtime"
	"sync"
)

// ParallelOptions contains parallel processing options
type ParallelOptions struct {
	MaxWorkers int
	Context    context.Context
}

// defaultParallelOptions returns default parallel processing options
func defaultParallelOptions() *ParallelOptions {
	return &ParallelOptions{
		MaxWorkers: runtime.NumCPU(),
		Context:    context.Background(),
	}
}

// WithMaxWorkers sets the maximum number of workers
func WithMaxWorkers(maxWorkers int) func(*ParallelOptions) {
	return func(o *ParallelOptions) {
		if maxWorkers > 0 {
			o.MaxWorkers = maxWorkers
		}
	}
}

// WithContext sets the context for cancellation and timeout support
func WithContext(ctx context.Context) func(*ParallelOptions) {
	return func(o *ParallelOptions) {
		if ctx != nil {
			o.Context = ctx
		}
	}
}

// parallelizeRows performs parallel operations on rows
func (df *DataFrame) parallelizeRows(processor func(row []string) []string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	ctx := opts.Context

	if len(df.Data) == 0 {
		return df, nil
	}

	numWorkers := opts.MaxWorkers
	if numWorkers > len(df.Data) {
		numWorkers = len(df.Data)
	}

	jobs := make(chan int, len(df.Data))
	results := make(chan struct {
		index int
		row   []string
	}, len(df.Data))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				row := processor(df.Data[j])
				results <- struct {
					index int
					row   []string
				}{index: j, row: row}
			}
		}()
	}

	for j := range df.Data {
		jobs <- j
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	newData := make([][]string, len(df.Data))
	copy(newData, df.Data)
	for r := range results {
		newData[r.index] = r.row
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	df.Data = newData
	return df, nil
}

// parallelizeColumns performs parallel operations on columns
func (df *DataFrame) parallelizeColumns(columnIndices []int, processor func(row []string, colIdx int) []string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	ctx := opts.Context

	if len(df.Data) == 0 {
		return df, nil
	}

	indices := make([]int, 0, len(columnIndices))
	for _, idx := range columnIndices {
		if idx >= 0 && idx < len(df.Headers) {
			indices = append(indices, idx)
		}
	}

	if len(indices) == 0 {
		return df, nil
	}

	totalJobs := len(df.Data) * len(indices)

	numWorkers := opts.MaxWorkers
	if numWorkers > totalJobs {
		numWorkers = totalJobs
	}

	type job struct {
		rowIdx  int
		colIdx  int
		rowData []string
	}
	jobs := make(chan job, totalJobs)
	results := make(chan struct {
		rowIdx int
		row    []string
	}, totalJobs)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				row := processor(j.rowData, j.colIdx)
				results <- struct {
					rowIdx int
					row    []string
				}{rowIdx: j.rowIdx, row: row}
			}
		}()
	}

	for i, row := range df.Data {
		for _, colIdx := range indices {
			jobs <- job{rowIdx: i, colIdx: colIdx, rowData: row}
		}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	newData := make([][]string, len(df.Data))
	copy(newData, df.Data)
	for r := range results {
		newData[r.rowIdx] = r.row
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	df.Data = newData
	return df, nil
}
