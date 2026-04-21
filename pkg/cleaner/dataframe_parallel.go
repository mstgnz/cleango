package cleaner

import (
	"fmt"
	"sync"
)

// TrimColumnsParallel cleans whitespace at the beginning and end of all values in all columns in parallel
func (df *DataFrame) TrimColumnsParallel(options ...func(*ParallelOptions)) (*DataFrame, error) {
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
	}, options...)
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

		t, err := parseDate(row[colIdx], layout)
		if err != nil {
			return row
		}

		row[colIdx] = t.Format(layout)
		return row
	}, options...)
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
	}, options...)
}

// CleanWithRegexParallel cleans values in the specified column with regex in parallel
func (df *DataFrame) CleanWithRegexParallel(column string, pattern string, replacement string, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	re, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}

	columnIndices := []int{colIndex}
	return df.parallelizeColumns(columnIndices, func(row []string, colIdx int) []string {
		row[colIdx] = re.ReplaceAllString(row[colIdx], replacement)
		return row
	}, options...)
}

// FilterOutliersParallel filters outlier values in the specified column in parallel
func (df *DataFrame) FilterOutliersParallel(column string, min, max float64, options ...func(*ParallelOptions)) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, column)
	}

	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	numWorkers := opts.MaxWorkers
	if numWorkers > len(df.Data) {
		numWorkers = len(df.Data)
	}

	jobs := make(chan int, len(df.Data))
	results := make(chan struct {
		index int
		keep  bool
	}, len(df.Data))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rowIndex := range jobs {
				select {
				case <-opts.Context.Done():
					return
				default:
				}

				value := df.Data[rowIndex][colIndex]
				if value == "" {
					results <- struct {
						index int
						keep  bool
					}{rowIndex, true}
					continue
				}

				num, err := parseFloat(value)
				if err != nil {
					results <- struct {
						index int
						keep  bool
					}{rowIndex, true}
					continue
				}

				results <- struct {
					index int
					keep  bool
				}{rowIndex, num >= min && num <= max}
			}
		}()
	}

	for i := range df.Data {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	keepMap := make(map[int]bool, len(df.Data))
	for result := range results {
		keepMap[result.index] = result.keep
	}

	if err := opts.Context.Err(); err != nil {
		return nil, err
	}

	var filteredData [][]string
	for i, row := range df.Data {
		if keepMap[i] {
			filteredData = append(filteredData, row)
		}
	}

	return &DataFrame{
		Headers: df.Headers,
		Data:    filteredData,
		Types:   df.Types,
	}, nil
}

// BatchProcessParallel applies multiple processors in parallel, then combines results in order
func (df *DataFrame) BatchProcessParallel(processors []func(*DataFrame) (*DataFrame, error), options ...func(*ParallelOptions)) (*DataFrame, error) {
	if len(processors) == 0 {
		return df, nil
	}

	opts := defaultParallelOptions()
	for _, option := range options {
		option(opts)
	}

	numWorkers := opts.MaxWorkers
	if numWorkers > len(processors) {
		numWorkers = len(processors)
	}

	type jobItem struct {
		index     int
		processor func(*DataFrame) (*DataFrame, error)
	}
	jobs := make(chan jobItem, len(processors))
	results := make(chan struct {
		index int
		df    *DataFrame
		err   error
	}, len(processors))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-opts.Context.Done():
					return
				default:
				}
				resultDF, err := j.processor(df.Copy())
				results <- struct {
					index int
					df    *DataFrame
					err   error
				}{j.index, resultDF, err}
			}
		}()
	}

	for i, processor := range processors {
		jobs <- jobItem{i, processor}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

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

	if err := opts.Context.Err(); err != nil {
		return nil, err
	}

	resultDF := df.Copy()
	for i := range processors {
		r, ok := resultMap[i]
		if !ok || r.err != nil {
			return nil, fmt.Errorf("missing or failed processor at index %d", i)
		}
		resultDF = r.df
	}

	return resultDF, nil
}

// Copy creates a deep copy of the DataFrame
func (df *DataFrame) Copy() *DataFrame {
	newData := make([][]string, len(df.Data))
	for i, row := range df.Data {
		newRow := make([]string, len(row))
		copy(newRow, row)
		newData[i] = newRow
	}

	newTypes := make(map[string]Type, len(df.Types))
	for k, v := range df.Types {
		newTypes[k] = v
	}

	return &DataFrame{
		Headers: append([]string{}, df.Headers...),
		Data:    newData,
		Types:   newTypes,
	}
}
