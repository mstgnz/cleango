package formats

import (
	"os"
	"testing"
)

func TestReadCSVToRaw(t *testing.T) {
	// Create temporary CSV file for testing
	tempFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test data
	csvContent := "Name,Age,City\nAli,30,İstanbul\nAyşe,25,Ankara\nMehmet,40,İzmir"
	if _, err := tempFile.WriteString(csvContent); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Read CSV file
	headers, data, err := ReadCSVToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("ReadCSVToRaw error: %v", err)
	}

	// Check headers
	expectedHeaders := []string{"Name", "Age", "City"}
	if len(headers) != len(expectedHeaders) {
		t.Errorf("Header count = %v, expected = %v", len(headers), len(expectedHeaders))
	}
	for i, h := range expectedHeaders {
		if headers[i] != h {
			t.Errorf("Header[%d] = %v, expected = %v", i, headers[i], h)
		}
	}

	// Check data
	if len(data) != 3 {
		t.Errorf("Row count = %v, expected = 3", len(data))
	}

	expectedData := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
		{"Mehmet", "40", "İzmir"},
	}

	for i, row := range expectedData {
		if len(data[i]) != len(row) {
			t.Errorf("Row %d column count = %v, expected = %v", i, len(data[i]), len(row))
			continue
		}

		for j, expected := range row {
			if data[i][j] != expected {
				t.Errorf("Data[%d][%d] = %v, expected = %v", i, j, data[i][j], expected)
			}
		}
	}
}

func TestReadCSVToRawWithOptions(t *testing.T) {
	// Create temporary CSV file for testing (semicolon delimiter)
	tempFile, err := os.CreateTemp("", "test_semicolon_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test data (semicolon delimiter)
	csvContent := "Name;Age;City\nAli;30;İstanbul\nAyşe;25;Ankara"
	if _, err := tempFile.WriteString(csvContent); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Read semicolon-delimited CSV file
	headers, data, err := ReadCSVToRaw(tempFile.Name(), WithDelimiter(';'))
	if err != nil {
		t.Fatalf("ReadCSVToRaw error: %v", err)
	}

	// Check headers
	expectedHeaders := []string{"Name", "Age", "City"}
	if len(headers) != len(expectedHeaders) {
		t.Errorf("Header count = %v, expected = %v", len(headers), len(expectedHeaders))
	}

	// Check data
	if len(data) != 2 {
		t.Errorf("Row count = %v, expected = 2", len(data))
	}

	// Check first row
	if data[0][0] != "Ali" || data[0][1] != "30" || data[0][2] != "İstanbul" {
		t.Errorf("First row = %v, expected = [Ali 30 İstanbul]", data[0])
	}
}

func TestWriteCSVFromRaw(t *testing.T) {
	// Test data
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	// Create temporary file
	tempFile, err := os.CreateTemp("", "test_write_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Write to CSV
	if err := WriteCSVFromRaw(headers, data, tempFile.Name()); err != nil {
		t.Fatalf("WriteCSVFromRaw error: %v", err)
	}

	// Read the written file and check
	readHeaders, readData, err := ReadCSVToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read written CSV file: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(headers) {
		t.Errorf("Read header count = %v, expected = %v", len(readHeaders), len(headers))
	}

	// Check data
	if len(readData) != len(data) {
		t.Errorf("Read row count = %v, expected = %v", len(readData), len(data))
	}

	for i, row := range data {
		for j, expected := range row {
			if readData[i][j] != expected {
				t.Errorf("Read data[%d][%d] = %v, expected = %v", i, j, readData[i][j], expected)
			}
		}
	}
}

// Mock DataFrame implementation
type mockDataFrame struct {
	headers []string
	data    [][]string
}

func (m *mockDataFrame) GetHeaders() []string {
	return m.headers
}

func (m *mockDataFrame) GetData() [][]string {
	return m.data
}

func TestWriteCSV(t *testing.T) {
	// Create mock DataFrame
	df := &mockDataFrame{
		headers: []string{"Name", "Age", "City"},
		data: [][]string{
			{"Ali", "30", "İstanbul"},
			{"Ayşe", "25", "Ankara"},
		},
	}

	// Create temporary file
	tempFile, err := os.CreateTemp("", "test_write_df_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Write DataFrame to CSV
	if err := WriteCSV(df, tempFile.Name()); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	// Read the written file and check
	readHeaders, readData, err := ReadCSVToRaw(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read written CSV file: %v", err)
	}

	// Check headers
	if len(readHeaders) != len(df.headers) {
		t.Errorf("Read header count = %v, expected = %v", len(readHeaders), len(df.headers))
	}

	// Check data
	if len(readData) != len(df.data) {
		t.Errorf("Read row count = %v, expected = %v", len(readData), len(df.data))
	}

	for i, row := range df.data {
		for j, expected := range row {
			if readData[i][j] != expected {
				t.Errorf("Read data[%d][%d] = %v, expected = %v", i, j, readData[i][j], expected)
			}
		}
	}
}
