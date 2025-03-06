package formats

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// DataFrame is an interface that defines the required methods for a data frame
type DataFrame interface {
	GetHeaders() []string
	GetData() [][]string
}

// CSVOptions contains CSV reading and writing options
type CSVOptions struct {
	Delimiter   rune
	LazyQuotes  bool
	SkipErrors  bool
	CommentChar rune
}

// CSVOption is a function type for setting CSV options
type CSVOption func(*CSVOptions)

// defaultCSVOptions returns default CSV options
func defaultCSVOptions() CSVOptions {
	return CSVOptions{
		Delimiter:   ',',
		LazyQuotes:  false,
		SkipErrors:  false,
		CommentChar: 0,
	}
}

// WithDelimiter sets the CSV delimiter
func WithDelimiter(delimiter rune) CSVOption {
	return func(o *CSVOptions) {
		o.Delimiter = delimiter
	}
}

// WithLazyQuotes determines whether to use relaxed rules for quotes
func WithLazyQuotes(lazyQuotes bool) CSVOption {
	return func(o *CSVOptions) {
		o.LazyQuotes = lazyQuotes
	}
}

// WithSkipErrors determines whether to skip lines with errors
func WithSkipErrors(skipErrors bool) CSVOption {
	return func(o *CSVOptions) {
		o.SkipErrors = skipErrors
	}
}

// WithComment sets the character used to identify comment lines
func WithComment(commentChar rune) CSVOption {
	return func(o *CSVOptions) {
		o.CommentChar = commentChar
	}
}

// ReadCSVToRaw reads a CSV file and returns raw data
func ReadCSVToRaw(filePath string, options ...CSVOption) ([]string, [][]string, error) {
	// Default settings
	opts := defaultCSVOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = opts.Delimiter
	reader.LazyQuotes = opts.LazyQuotes
	reader.Comment = opts.CommentChar

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Read data
	var rows [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if opts.SkipErrors {
				continue
			}
			return nil, nil, fmt.Errorf("failed to read CSV row: %w", err)
		}
		rows = append(rows, row)
	}

	return headers, rows, nil
}

// WriteCSVFromRaw writes raw data to a CSV file
func WriteCSVFromRaw(headers []string, data [][]string, filePath string, options ...CSVOption) error {
	// Default settings
	opts := defaultCSVOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	writer.Comma = opts.Delimiter

	// Write headers
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write CSV headers: %w", err)
	}

	// Write data
	for _, row := range data {
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Flush buffer
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("CSV writer error: %w", err)
	}

	return nil
}

// WriteCSV writes DataFrame to a CSV file
func WriteCSV(df DataFrame, filePath string, options ...CSVOption) error {
	return WriteCSVFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}
