package cleaner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mstgnz/cleango/pkg/formats"
)

func TestReadCSV(t *testing.T) {
	// Create temporary CSV file for testing
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test.csv")

	// Write test data
	err := os.WriteFile(tempFile, []byte(`name,age,city
John,30,New York
Jane,25,London
Bob,40,Paris
`), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Read CSV file
	df, err := ReadCSV(tempFile)
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Check headers
	expectedHeaders := []string{"name", "age", "city"}
	if len(df.Headers) != len(expectedHeaders) {
		t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(df.Headers))
	}
	for i, h := range expectedHeaders {
		if df.Headers[i] != h {
			t.Errorf("Expected header %s at position %d, got %s", h, i, df.Headers[i])
		}
	}

	// Check data
	if len(df.Data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(df.Data))
	}
	expectedData := [][]string{
		{"John", "30", "New York"},
		{"Jane", "25", "London"},
		{"Bob", "40", "Paris"},
	}
	for i, row := range expectedData {
		for j, cell := range row {
			if df.Data[i][j] != cell {
				t.Errorf("Expected %s at position (%d,%d), got %s", cell, i, j, df.Data[i][j])
			}
		}
	}
}

func TestReadCSVWithSemicolon(t *testing.T) {
	// Create temporary CSV file with semicolon delimiter
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_semicolon.csv")

	// Write test data with semicolon delimiter
	err := os.WriteFile(tempFile, []byte(`name;age;city
John;30;New York
Jane;25;London
Bob;40;Paris
`), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Read CSV file with semicolon delimiter
	df, err := ReadCSV(tempFile, formats.WithDelimiter(';'))
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Check headers
	expectedHeaders := []string{"name", "age", "city"}
	if len(df.Headers) != len(expectedHeaders) {
		t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(df.Headers))
	}

	// Check data
	if len(df.Data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(df.Data))
	}

	// Check first row
	if df.Data[0][0] != "John" || df.Data[0][1] != "30" || df.Data[0][2] != "New York" {
		t.Errorf("First row data doesn't match expected values")
	}
}

func TestWriteCSV(t *testing.T) {
	// Create test DataFrame
	headers := []string{"name", "age", "city"}
	data := [][]string{
		{"John", "30", "New York"},
		{"Jane", "25", "London"},
		{"Bob", "40", "Paris"},
	}
	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Create temporary file
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "output.csv")

	// Write DataFrame to CSV
	err = df.WriteCSV(tempFile)
	if err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Read the written file and verify
	readDf, err := ReadCSV(tempFile)
	if err != nil {
		t.Fatalf("Failed to read written CSV: %v", err)
	}

	// Check headers
	if len(readDf.Headers) != len(headers) {
		t.Errorf("Expected %d headers, got %d", len(headers), len(readDf.Headers))
	}

	// Check data
	if len(readDf.Data) != len(data) {
		t.Errorf("Expected %d rows, got %d", len(data), len(readDf.Data))
	}
}

func TestWriteCSVWithSemicolon(t *testing.T) {
	// Create test DataFrame
	headers := []string{"name", "age", "city"}
	data := [][]string{
		{"John", "30", "New York"},
		{"Jane", "25", "London"},
		{"Bob", "40", "Paris"},
	}
	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Create temporary file
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "output_semicolon.csv")

	// Write DataFrame to CSV with semicolon delimiter
	err = df.WriteCSV(tempFile, formats.WithDelimiter(';'))
	if err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Read the written file and verify (using the same delimiter)
	readDf, err := ReadCSV(tempFile, formats.WithDelimiter(';'))
	if err != nil {
		t.Fatalf("Failed to read written CSV: %v", err)
	}

	// Check data
	if len(readDf.Data) != len(data) {
		t.Errorf("Expected %d rows, got %d", len(data), len(readDf.Data))
	}
}
