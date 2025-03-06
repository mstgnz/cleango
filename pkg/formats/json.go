package formats

import (
	"encoding/json"
	"fmt"
	"os"
)

// JSONOptions contains JSON reading and writing options
type JSONOptions struct {
	Pretty bool // Format JSON nicely
}

// JSONOption is a function type for setting JSON options
type JSONOption func(*JSONOptions)

// defaultJSONOptions returns default JSON options
func defaultJSONOptions() JSONOptions {
	return JSONOptions{
		Pretty: false,
	}
}

// WithPretty determines whether JSON should be nicely formatted
func WithPretty(pretty bool) JSONOption {
	return func(o *JSONOptions) {
		o.Pretty = pretty
	}
}

// ReadJSONToRaw reads a JSON file and returns raw data
func ReadJSONToRaw(filePath string, options ...JSONOption) ([]string, [][]string, error) {
	// Default settings
	opts := defaultJSONOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open JSON file: %w", err)
	}
	defer file.Close()

	// Parse JSON
	var data []map[string]interface{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Collect headers
	headers := make(map[string]bool)

	// Iterate through all records to collect unique headers
	for _, record := range data {
		for key := range record {
			headers[key] = true
		}
	}

	// Convert headers map to slice
	headerSlice := make([]string, 0, len(headers))
	for header := range headers {
		headerSlice = append(headerSlice, header)
	}

	// Convert data
	rows := make([][]string, len(data))
	for i, record := range data {
		row := make([]string, len(headerSlice))
		for j, header := range headerSlice {
			if val, ok := record[header]; ok {
				row[j] = formatJSONValue(val)
			} else {
				row[j] = "" // Empty string for missing values
			}
		}
		rows[i] = row
	}

	return headerSlice, rows, nil
}

// WriteJSONFromRaw writes raw data to a JSON file
func WriteJSONFromRaw(headers []string, data [][]string, filePath string, options ...JSONOption) error {
	// Default settings
	opts := defaultJSONOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Convert data to JSON format
	jsonData := make([]map[string]interface{}, len(data))
	for i, row := range data {
		record := make(map[string]interface{})
		for j, header := range headers {
			if j < len(row) {
				record[header] = row[j]
			}
		}
		jsonData[i] = record
	}

	// Convert to JSON
	var jsonBytes []byte
	var err error
	if opts.Pretty {
		jsonBytes, err = json.MarshalIndent(jsonData, "", "  ")
	} else {
		jsonBytes, err = json.Marshal(jsonData)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("failed to write JSON file: %w", err)
	}

	return nil
}

// WriteJSON writes DataFrame to a JSON file
func WriteJSON(df interface {
	GetHeaders() []string
	GetData() [][]string
}, filePath string, options ...JSONOption) error {
	return WriteJSONFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}

// formatJSONValue converts a JSON value to string
func formatJSONValue(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case float64:
		// For integers, don't show decimal part
		if v == float64(int(v)) {
			return fmt.Sprintf("%d", int(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case map[string]interface{}:
		// Nested JSON object
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	case []interface{}:
		// JSON array
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	default:
		// Other types
		return fmt.Sprintf("%v", v)
	}
}
