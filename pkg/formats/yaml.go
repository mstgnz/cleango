package formats

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// YAMLOptions contains YAML reading and writing options
type YAMLOptions struct {
	Pretty bool // Format YAML nicely
}

// YAMLOption is a function type for setting YAML options
type YAMLOption func(*YAMLOptions)

// defaultYAMLOptions returns default YAML options
func defaultYAMLOptions() YAMLOptions {
	return YAMLOptions{
		Pretty: false,
	}
}

// WithYAMLPretty determines whether YAML should be nicely formatted
func WithYAMLPretty(pretty bool) YAMLOption {
	return func(o *YAMLOptions) {
		o.Pretty = pretty
	}
}

// ReadYAMLToRaw reads a YAML file and returns raw data
func ReadYAMLToRaw(filePath string, options ...YAMLOption) ([]string, [][]string, error) {
	// Default settings
	opts := defaultYAMLOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open YAML file: %w", err)
	}
	defer file.Close()

	// Parse YAML
	var data []map[string]interface{}
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, nil, fmt.Errorf("failed to parse YAML: %w", err)
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
				row[j] = formatYAMLValue(val)
			} else {
				row[j] = "" // Empty string for missing values
			}
		}
		rows[i] = row
	}

	return headerSlice, rows, nil
}

// WriteYAMLFromRaw writes raw data to a YAML file
func WriteYAMLFromRaw(headers []string, data [][]string, filePath string, options ...YAMLOption) error {
	// Default settings
	opts := defaultYAMLOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Convert data to YAML format
	yamlData := make([]map[string]interface{}, len(data))
	for i, row := range data {
		record := make(map[string]interface{})
		for j, header := range headers {
			if j < len(row) {
				record[header] = row[j]
			}
		}
		yamlData[i] = record
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create YAML file: %w", err)
	}
	defer file.Close()

	// Create YAML encoder
	encoder := yaml.NewEncoder(file)
	if opts.Pretty {
		encoder.SetIndent(2)
	}
	defer encoder.Close()

	// Write data
	if err := encoder.Encode(yamlData); err != nil {
		return fmt.Errorf("failed to encode YAML: %w", err)
	}

	return nil
}

// WriteYAML writes DataFrame to a YAML file
func WriteYAML(df interface {
	GetHeaders() []string
	GetData() [][]string
}, filePath string, options ...YAMLOption) error {
	return WriteYAMLFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}

// formatYAMLValue converts a YAML value to string
func formatYAMLValue(value interface{}) string {
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
		// Nested YAML object
		yamlBytes, err := yaml.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(yamlBytes)
	case []interface{}:
		// YAML array
		yamlBytes, err := yaml.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(yamlBytes)
	default:
		// Other types
		return fmt.Sprintf("%v", v)
	}
}
