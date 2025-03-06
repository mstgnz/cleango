package cleaner

import (
	"runtime"
	"sync"
)

// ParallelOptions contains parallel processing options
type ParallelOptions struct {
	MaxWorkers int // Maximum number of workers
}

// defaultParallelOptions returns default parallel processing options
func defaultParallelOptions() *ParallelOptions {
	return &ParallelOptions{
		MaxWorkers: runtime.NumCPU(), // Default to number of CPU cores
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

// parallelizeRows performs parallel operations on rows
func (df *DataFrame) parallelizeRows(processor func(row []string) []string, options ...func(*ParallelOptions)) *DataFrame {
	// Set options
	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	// Don't process if no data
	if len(df.Data) == 0 {
		return df
	}

	// Determine number of workers
	numWorkers := opts.MaxWorkers
	if numWorkers > len(df.Data) {
		numWorkers = len(df.Data)
	}

	// Create job channel and result channel
	jobs := make(chan int, len(df.Data))
	results := make(chan struct {
		index int
		row   []string
	}, len(df.Data))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Process row
				row := processor(df.Data[j])
				// Send result
				results <- struct {
					index int
					row   []string
				}{
					index: j,
					row:   row,
				}
			}
		}()
	}

	// Send jobs
	for j := range df.Data {
		jobs <- j
	}
	close(jobs)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	newData := make([][]string, len(df.Data))
	for r := range results {
		newData[r.index] = r.row
	}

	// Update DataFrame
	df.Data = newData
	return df
}

// parallelizeColumns performs parallel operations on columns
func (df *DataFrame) parallelizeColumns(columnIndices []int, processor func(row []string, colIdx int) []string, options ...func(*ParallelOptions)) *DataFrame {
	// Set options
	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	// Don't process if no data
	if len(df.Data) == 0 {
		return df
	}

	// Find column indices
	indices := make([]int, 0, len(columnIndices))
	for _, idx := range columnIndices {
		if idx >= 0 && idx < len(df.Headers) {
			indices = append(indices, idx)
		}
	}

	// Don't process if no columns to process
	if len(indices) == 0 {
		return df
	}

	// Calculate total number of jobs (rows * columns to process)
	totalJobs := len(df.Data) * len(indices)

	// Determine number of workers
	numWorkers := opts.MaxWorkers
	if numWorkers > totalJobs {
		numWorkers = totalJobs
	}

	// Create job channel and result channel
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

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Process cell
				row := processor(j.rowData, j.colIdx)
				// Send result
				results <- struct {
					rowIdx int
					row    []string
				}{
					rowIdx: j.rowIdx,
					row:    row,
				}
			}
		}()
	}

	// Send jobs
	for i, row := range df.Data {
		for _, colIdx := range indices {
			jobs <- job{
				rowIdx:  i,
				colIdx:  colIdx,
				rowData: row,
			}
		}
	}
	close(jobs)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	newData := make([][]string, len(df.Data))
	copy(newData, df.Data)

	for r := range results {
		newData[r.rowIdx] = r.row
	}

	// Update DataFrame
	df.Data = newData
	return df
}
