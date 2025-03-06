package formats

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetOptions, Parquet includes read and write options
type ParquetOptions struct {
	Compression parquet.CompressionCodec // Compression algorithm
}

// ParquetOption, Function type for setting Parquet options
type ParquetOption func(*ParquetOptions)

// defaultParquetOptions, returns the default Parquet options
func defaultParquetOptions() *ParquetOptions {
	return &ParquetOptions{
		Compression: parquet.CompressionCodec_SNAPPY,
	}
}

// WithCompression, Parquet determines the compression algorithm
func WithCompression(compression parquet.CompressionCodec) ParquetOption {
	return func(o *ParquetOptions) {
		o.Compression = compression
	}
}

// ParquetRecord, Represents a record in a Parquet file
type ParquetRecord map[string]interface{}

// ReadParquetToRaw, Reads the Parquet file and returns the raw data
func ReadParquetToRaw(filePath string, options ...ParquetOption) ([]string, [][]string, error) {
	// Varsayılan ayarlar
	opts := defaultParquetOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(opts)
	}

	// Open Parquet file
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("parquet file could not be opened: %w", err)
	}
	defer fr.Close()

	// Create parquet reader
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, nil, fmt.Errorf("parquet reader could not be created: %w", err)
	}
	defer pr.ReadStop()

	// Get the number of records
	numRows := int(pr.GetNumRows())
	if numRows == 0 {
		return nil, nil, fmt.Errorf("parquet file is empty")
	}

	// Verileri oku
	records := make([]ParquetRecord, numRows)
	if err := pr.Read(&records); err != nil {
		return nil, nil, fmt.Errorf("parquet data could not be read: %w", err)
	}

	// Collect titles (from the first record)
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("parquet records not found")
	}

	headers := make([]string, 0)
	for header := range records[0] {
		headers = append(headers, header)
	}

	// Convert data to string matrix
	data := make([][]string, numRows)
	for i, record := range records {
		row := make([]string, len(headers))
		for j, header := range headers {
			if val, ok := record[header]; ok {
				row[j] = fmt.Sprintf("%v", val)
			} else {
				row[j] = "" // Empty string for missing values
			}
		}
		data[i] = row
	}

	return headers, data, nil
}

// WriteParquetFromRaw, writes raw data to Parquet file
func WriteParquetFromRaw(headers []string, data [][]string, filePath string, options ...ParquetOption) error {
	// Default settings
	opts := defaultParquetOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(opts)
	}

	// Create Parquet file
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("parquet file could not be created: %w", err)
	}
	defer fw.Close()

	// Create schematic for Parquet printer
	schema := generateParquetSchema(headers, data)

	// Parquet yazıcı oluştur
	pw, err := writer.NewParquetWriter(fw, schema, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet printer: %w", err)
	}

	// Set compression algorithm
	pw.CompressionType = opts.Compression

	// Transform and write data
	for _, row := range data {
		record := make(ParquetRecord)
		for i, header := range headers {
			if i < len(row) {
				// Determine the data type and convert appropriately
				value := row[i]
				if isNumeric(value) {
					// Numerical value
					if strings.Contains(value, ".") {
						// Decimal number
						if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
							record[header] = floatVal
							continue
						}
					} else {
						// Whole number
						if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
							record[header] = intVal
							continue
						}
					}
				}

				// Boolean value
				if value == "true" || value == "false" {
					if boolVal, err := strconv.ParseBool(value); err == nil {
						record[header] = boolVal
						continue
					}
				}

				// String value
				record[header] = value
			} else {
				record[header] = ""
			}
		}

		if err := pw.Write(record); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}
	}

	// Yazıcıyı kapat
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("parquet printer failed to close: %w", err)
	}

	return nil
}

// WriteParquet, Writes DataFrame to Parquet file
func WriteParquet(df DataFrame, filePath string, options ...ParquetOption) error {
	return WriteParquetFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}

// generateParquetSchema, creates Parquet schema for given headers and data
func generateParquetSchema(headers []string, data [][]string) interface{} {
	// Veri tiplerini belirle
	types := make(map[string]reflect.Type)
	for _, header := range headers {
		types[header] = reflect.TypeOf("")
	}

	// Identify the data types by examining the first few lines
	sampleSize := 10
	if len(data) < sampleSize {
		sampleSize = len(data)
	}

	for i := 0; i < sampleSize; i++ {
		row := data[i]
		for j, header := range headers {
			if j < len(row) {
				value := row[j]
				if value == "" {
					continue
				}

				// Check for numeric value
				if isNumeric(value) {
					if strings.Contains(value, ".") {
						types[header] = reflect.TypeOf(float64(0))
					} else {
						types[header] = reflect.TypeOf(int64(0))
					}
					continue
				}

				// Check if it is a Boolean value
				if value == "true" || value == "false" {
					types[header] = reflect.TypeOf(bool(false))
					continue
				}
			}
		}
	}

	// Create dynamic schema
	record := make(ParquetRecord)
	for _, header := range headers {
		switch types[header].Kind() {
		case reflect.Int64:
			record[header] = int64(0)
		case reflect.Float64:
			record[header] = float64(0)
		case reflect.Bool:
			record[header] = false
		default:
			record[header] = ""
		}
	}

	return record
}
