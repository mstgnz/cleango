package formats

import (
	"os"
	"testing"
)

func TestReadJSONToRaw(t *testing.T) {
	// Create temporary JSON file for testing
	tempFile, err := os.CreateTemp("", "test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary JSON file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test data
	jsonContent := `[
		{"Name": "Ali", "Age": 30, "City": "İstanbul"},
		{"Name": "Ayşe", "Age": 25, "City": "Ankara"},
		{"Name": "Mehmet", "Age": 40, "City": "İzmir"}
	]`
	if _, err := tempFile.WriteString(jsonContent); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Read JSON file
	headers, data, err := ReadJSONToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("ReadJSONToRaw error: %v", err)
	}

	// Check if headers exist (order may not matter)
	expectedHeaders := []string{"Name", "Age", "City"}
	for _, expected := range expectedHeaders {
		found := false
		for _, h := range headers {
			if h == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Header '%s' not found", expected)
		}
	}

	// Check data
	if len(data) != 3 {
		t.Errorf("Row count = %v, expected = 3", len(data))
	}

	// Find the index of the "Name" column
	nameIndex := -1
	for i, h := range headers {
		if h == "Name" {
			nameIndex = i
			break
		}
	}

	if nameIndex == -1 {
		t.Fatalf("'Name' column not found")
	}

	// Check names
	expectedNames := map[string]bool{
		"Ali":    true,
		"Ayşe":   true,
		"Mehmet": true,
	}

	for _, row := range data {
		if !expectedNames[row[nameIndex]] {
			t.Errorf("Unexpected name: %s", row[nameIndex])
		}
	}
}

func TestWriteJSONFromRaw(t *testing.T) {
	// Test data
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	// Create temporary file
	tempFile, err := os.CreateTemp("", "test_write_*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Write as JSON
	if err := WriteJSONFromRaw(headers, data, tempFile.Name()); err != nil {
		t.Fatalf("WriteJSONFromRaw error: %v", err)
	}

	// Read the written file and check
	readHeaders, readData, err := ReadJSONToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read written JSON file: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(headers) {
		t.Errorf("Read header count = %v, expected = %v", len(readHeaders), len(headers))
	}

	// Check data
	if len(readData) != len(data) {
		t.Errorf("Read row count = %v, expected = %v", len(readData), len(data))
	}

	// Find the index of the Name column (in both original and read data)
	nameIndexOrig := -1
	for i, h := range headers {
		if h == "Name" {
			nameIndexOrig = i
			break
		}
	}

	nameIndexRead := -1
	for i, h := range readHeaders {
		if h == "Name" {
			nameIndexRead = i
			break
		}
	}

	if nameIndexOrig == -1 || nameIndexRead == -1 {
		t.Fatalf("'Name' column not found")
	}

	// Check names
	expectedNames := map[string]bool{
		"Ali":  true,
		"Ayşe": true,
	}

	for _, row := range readData {
		if !expectedNames[row[nameIndexRead]] {
			t.Errorf("Unexpected name: %s", row[nameIndexRead])
		}
	}
}

func TestWriteJSON(t *testing.T) {
	// Create mock DataFrame
	df := &mockDataFrame{
		headers: []string{"Name", "Age", "City"},
		data: [][]string{
			{"Ali", "30", "İstanbul"},
			{"Ayşe", "25", "Ankara"},
		},
	}

	// Create temporary file
	tempFile, err := os.CreateTemp("", "test_write_df_*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Write DataFrame as JSON
	if err := WriteJSON(df, tempFile.Name()); err != nil {
		t.Fatalf("WriteJSON error: %v", err)
	}

	// Read the written file and check
	readHeaders, readData, err := ReadJSONToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read written JSON file: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(df.headers) {
		t.Errorf("Read header count = %v, expected = %v", len(readHeaders), len(df.headers))
	}

	// Check data
	if len(readData) != len(df.data) {
		t.Errorf("Read row count = %v, expected = %v", len(readData), len(df.data))
	}

	// Find the index of the Name column
	nameIndexOrig := -1
	for i, h := range df.headers {
		if h == "Name" {
			nameIndexOrig = i
			break
		}
	}

	nameIndexRead := -1
	for i, h := range readHeaders {
		if h == "Name" {
			nameIndexRead = i
			break
		}
	}

	if nameIndexOrig == -1 || nameIndexRead == -1 {
		t.Fatalf("'Name' column not found")
	}

	// Check names
	expectedNames := map[string]bool{
		"Ali":  true,
		"Ayşe": true,
	}

	for _, row := range readData {
		if !expectedNames[row[nameIndexRead]] {
			t.Errorf("Unexpected name: %s", row[nameIndexRead])
		}
	}
}
