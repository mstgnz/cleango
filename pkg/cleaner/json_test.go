package cleaner

import (
	"os"
	"testing"
)

func TestReadJSON(t *testing.T) {
	// Create a temporary JSON file for testing
	tempFile, err := os.CreateTemp("", "test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
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
	df, err := ReadJSON(tempFile.Name())
	if err != nil {
		t.Fatalf("ReadJSON error: %v", err)
	}

	// Check headers
	headers := df.GetHeaders()
	expectedHeaders := []string{"Name", "Age", "City"}

	// Check if headers exist (order is not important)
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
	data := df.GetData()
	if len(data) != 3 {
		t.Errorf("Row count = %v, expected = 3", len(data))
	}

	// Find the index of the Name column
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

func TestWriteJSON(t *testing.T) {
	// Create a DataFrame for testing
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "test_write_*.json")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Write the DataFrame to JSON
	if err := df.WriteJSON(tempFile.Name()); err != nil {
		t.Fatalf("WriteJSON error: %v", err)
	}

	// Read the written file and check
	readDF, err := ReadJSON(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read written JSON file: %v", err)
	}

	// Check headers
	readHeaders := readDF.GetHeaders()
	if len(readHeaders) != len(headers) {
		t.Errorf("Read header count = %v, expected = %v", len(readHeaders), len(headers))
	}

	// Check data
	readData := readDF.GetData()
	if len(readData) != len(data) {
		t.Errorf("Read row count = %v, expected = %v", len(readData), len(data))
	}

	// Find the index of the Name column (in both the original and read DataFrames)
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
