package formats

import (
	"os"
	"testing"
)

// TestDataFrame is a simple implementation of the DataFrame interface for testing
type TestDataFrame struct {
	Headers []string
	Data    [][]string
}

func (df *TestDataFrame) GetHeaders() []string {
	return df.Headers
}

func (df *TestDataFrame) GetData() [][]string {
	return df.Data
}

func TestReadWriteXML(t *testing.T) {
	// Test data
	headers := []string{"id", "name", "email", "age"}
	data := [][]string{
		{"1", "John Doe", "john@example.com", "30"},
		{"2", "Jane Smith", "jane@example.com", "25"},
		{"3", "Bob Johnson", "bob@example.com", "40"},
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "test_*.xml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFileName)

	// Write data to XML file
	err = WriteXMLFromRaw(headers, data, tempFileName, WithXMLPretty(true))
	if err != nil {
		t.Fatalf("Failed to write XML: %v", err)
	}

	// Read data from XML file
	readHeaders, readData, err := ReadXMLToRaw(tempFileName)
	if err != nil {
		t.Fatalf("Failed to read XML: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(headers) {
		t.Errorf("Header count mismatch: got %d, want %d", len(readHeaders), len(headers))
	}

	// Create a map for easier comparison since XML doesn't guarantee order
	headerMap := make(map[string]bool)
	for _, h := range headers {
		headerMap[h] = true
	}

	for _, h := range readHeaders {
		if !headerMap[h] {
			t.Errorf("Unexpected header: %s", h)
		}
	}

	// Check data
	if len(readData) != len(data) {
		t.Errorf("Row count mismatch: got %d, want %d", len(readData), len(data))
	}

	// Create a map for data comparison
	originalData := make(map[string]map[string]string)
	for _, row := range data {
		record := make(map[string]string)
		for i, header := range headers {
			record[header] = row[i]
		}
		originalData[row[0]] = record // Using ID as key
	}

	readDataMap := make(map[string]map[string]string)
	for _, row := range readData {
		record := make(map[string]string)
		for i, header := range readHeaders {
			if i < len(row) {
				record[header] = row[i]
			}
		}

		// Find ID column index in readHeaders
		idIndex := -1
		for i, h := range readHeaders {
			if h == "id" {
				idIndex = i
				break
			}
		}

		if idIndex >= 0 && idIndex < len(row) {
			readDataMap[row[idIndex]] = record
		}
	}

	// Compare data
	for id, origRecord := range originalData {
		readRecord, ok := readDataMap[id]
		if !ok {
			t.Errorf("Missing record with ID %s", id)
			continue
		}

		for header, origValue := range origRecord {
			readValue, ok := readRecord[header]
			if !ok {
				t.Errorf("Missing field %s in record with ID %s", header, id)
				continue
			}

			if readValue != origValue {
				t.Errorf("Value mismatch for ID %s, field %s: got %s, want %s", id, header, readValue, origValue)
			}
		}
	}
}

func TestXMLWithDataFrame(t *testing.T) {
	// Create a test DataFrame
	df := &TestDataFrame{
		Headers: []string{"id", "name", "email", "age"},
		Data: [][]string{
			{"1", "John Doe", "john@example.com", "30"},
			{"2", "Jane Smith", "jane@example.com", "25"},
			{"3", "Bob Johnson", "bob@example.com", "40"},
		},
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "test_df_*.xml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFileName)

	// Write DataFrame to XML file
	err = WriteXML(df, tempFileName, WithXMLRootElement("users"), WithXMLItemElement("user"))
	if err != nil {
		t.Fatalf("Failed to write XML: %v", err)
	}

	// Read data from XML file
	readHeaders, readData, err := ReadXMLToRaw(tempFileName, WithXMLRootElement("users"), WithXMLItemElement("user"))
	if err != nil {
		t.Fatalf("Failed to read XML: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(df.Headers) {
		t.Errorf("Header count mismatch: got %d, want %d", len(readHeaders), len(df.Headers))
	}

	// Check data
	if len(readData) != len(df.Data) {
		t.Errorf("Row count mismatch: got %d, want %d", len(readData), len(df.Data))
	}
}
