package cleaner

import (
	"fmt"
	"sync"
)

// TrimColumnsParallel cleans whitespace at the beginning and end of all values in all columns in parallel
func (df *DataFrame) TrimColumnsParallel(options ...func(*ParallelOptions)) *DataFrame {
	return df.parallelizeRows(func(row []string) []string {
		for i := range row {
			row[i] = trimSpace(row[i])
		}
		return row
	}, options...)
}

// ReplaceNullsParallel replaces empty values with the specified default value in parallel
func (df *DataFrame) ReplaceNullsParallel(column string, defaultValue string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	columnIndices := []int{colIndex}
	return df.parallelizeColumns(columnIndices, func(row []string, colIdx int) []string {
		if row[colIdx] == "" {
			row[colIdx] = defaultValue
		}
		return row
	}, options...), nil
}

// CleanDatesParallel converts date values in the specified column to the specified format in parallel
func (df *DataFrame) CleanDatesParallel(column string, layout string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	columnIndices := []int{colIndex}
	return df.parallelizeColumns(columnIndices, func(row []string, colIdx int) []string {
		if row[colIdx] == "" {
			return row
		}

		// Parse the date and convert to the specified format
		t, err := parseDate(row[colIdx], layout)
		if err != nil {
			// Keep the original value in case of error
			return row
		}

		// Convert to the specified format
		row[colIdx] = t.Format(layout)
		return row
	}, options...), nil
}

// NormalizeCaseParallel converts text in the specified column to upper/lower case in parallel
func (df *DataFrame) NormalizeCaseParallel(column string, toUpper bool, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	columnIndices := []int{colIndex}
	return df.parallelizeColumns(columnIndices, func(row []string, colIdx int) []string {
		if toUpper {
			row[colIdx] = toUpperCase(row[colIdx])
		} else {
			row[colIdx] = toLowerCase(row[colIdx])
		}
		return row
	}, options...), nil
}

// CleanWithRegexParallel cleans values in the specified column with regex in parallel
func (df *DataFrame) CleanWithRegexParallel(column string, pattern string, replacement string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	// Compile regex
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}

	columnIndices := []int{colIndex}
	return df.parallelizeColumns(columnIndices, func(row []string, colIdx int) []string {
		row[colIdx] = re.ReplaceAllString(row[colIdx], replacement)
		return row
	}, options...), nil
}

// FilterOutliersParallel filters outlier values in the specified column in parallel
func (df *DataFrame) FilterOutliersParallel(column string, min, max float64, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	// Collect filtered data
	var filteredData [][]string
	var mutex sync.Mutex

	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
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
		keep  bool
	}, len(df.Data))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rowIndex := range jobs {
				// Check value
				value := df.Data[rowIndex][colIndex]
				if value == "" {
					results <- struct {
						index int
						keep  bool
					}{rowIndex, true} // Keep empty values
					continue
				}

				// Convert to number
				num, err := parseFloat(value)
				if err != nil {
					results <- struct {
						index int
						keep  bool
					}{rowIndex, true} // Keep non-numeric values
					continue
				}

				// Check if in range
				inRange := num >= min && num <= max
				results <- struct {
					index int
					keep  bool
				}{rowIndex, inRange}
			}
		}()
	}

	// Send jobs
	for i := range df.Data {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for result := range results {
		if result.keep {
			mutex.Lock()
			filteredData = append(filteredData, df.Data[result.index])
			mutex.Unlock()
		}
	}

	// Create new DataFrame
	result := &DataFrame{
		Headers: df.Headers,
		Data:    filteredData,
		Types:   df.Types,
	}

	return result, nil
}

// BatchProcessParallel applies multiple processes in parallel
func (df *DataFrame) BatchProcessParallel(processors []func(*DataFrame) (*DataFrame, error), options ...func(*ParallelOptions)) (*DataFrame, error) {
	if len(processors) == 0 {
		return df, nil
	}

	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	// Don't process if no processors
	if len(processors) == 0 {
		return df, nil
	}

	// Determine number of workers
	numWorkers := opts.MaxWorkers
	if numWorkers > len(processors) {
		numWorkers = len(processors)
	}

	// Create job channel and result channel
	type job struct {
		index     int
		processor func(*DataFrame) (*DataFrame, error)
	}
	jobs := make(chan job, len(processors))
	results := make(chan struct {
		index int
		df    *DataFrame
		err   error
	}, len(processors))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Apply process
				resultDF, err := j.processor(df.Copy())
				// Send result
				results <- struct {
					index int
					df    *DataFrame
					err   error
				}{j.index, resultDF, err}
			}
		}()
	}

	// Send jobs
	for i, processor := range processors {
		jobs <- job{i, processor}
	}
	close(jobs)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	type result struct {
		df  *DataFrame
		err error
	}
	resultMap := make(map[int]result)
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		resultMap[r.index] = result{r.df, r.err}
	}

	// Combine results in order
	resultDF := df.Copy()
	for i := 0; i < len(processors); i++ {
		r, ok := resultMap[i]
		if !ok || r.err != nil {
			return nil, fmt.Errorf("missing or failed processor at index %d", i)
		}
		resultDF = r.df
	}

	return resultDF, nil
}

// Copy creates a copy of the DataFrame
func (df *DataFrame) Copy() *DataFrame {
	newData := make([][]string, len(df.Data))
	for i, row := range df.Data {
		newRow := make([]string, len(row))
		copy(newRow, row)
		newData[i] = newRow
	}

	newTypes := make(map[string]Type)
	for k, v := range df.Types {
		newTypes[k] = v
	}

	return &DataFrame{
		Headers: append([]string{}, df.Headers...),
		Data:    newData,
		Types:   newTypes,
	}
}
