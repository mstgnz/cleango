package cleaner

import (
	"os"
	"testing"

	"github.com/mstgnz/cleango/pkg/formats"
)

func TestReadExcel(t *testing.T) {
	// This test requires a real Excel file, so we only test basic error cases

	// Check for non-existent file
	_, err := ReadExcel("olmayan_dosya.xlsx")
	if err == nil {
		t.Errorf("Expected error for non-existent Excel file, but got none")
	}
}

func TestWriteExcel(t *testing.T) {
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
	tempFile, err := os.CreateTemp("", "test_write_*.xlsx")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Write DataFrame to Excel
	if err := df.WriteExcel(tempFile.Name()); err != nil {
		t.Fatalf("WriteExcel error: %v", err)
	}

	// Check if the file exists
	if _, err := os.Stat(tempFile.Name()); os.IsNotExist(err) {
		t.Errorf("Excel file not created")
	}
}

func TestWriteExcelWithOptions(t *testing.T) {
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
	tempFile, err := os.CreateTemp("", "test_write_sheet_*.xlsx")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Write DataFrame to Excel with a custom sheet name
	if err := df.WriteExcel(tempFile.Name(), formats.WithSheetName("TestSheet")); err != nil {
		t.Fatalf("WriteExcel error: %v", err)
	}

	// Check if the file exists
	if _, err := os.Stat(tempFile.Name()); os.IsNotExist(err) {
		t.Errorf("Excel file not created")
	}
}
