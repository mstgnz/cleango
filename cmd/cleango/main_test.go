package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetFileFormat(t *testing.T) {
	tests := []struct {
		path   string
		expect string
	}{
		{"data.csv", "csv"},
		{"data.CSV", "csv"},
		{"DATA.CSV", "csv"},
		{"data.json", "json"},
		{"data.JSON", "json"},
		{"data.xlsx", "excel"},
		{"data.xls", "excel"},
		{"data.XLSX", "excel"},
		{"data.parquet", "parquet"},
		{"data.PARQUET", "parquet"},
		{"data.txt", ""},
		{"data.xml", ""},
		{"data", ""},
		{"/path/to/file.csv", "csv"},
		{"../relative/path.json", "json"},
	}

	for _, tt := range tests {
		got := getFileFormat(tt.path)
		if got != tt.expect {
			t.Errorf("getFileFormat(%q) = %q, want %q", tt.path, got, tt.expect)
		}
	}
}

func TestGetFileFormat_AllSupportedFormats(t *testing.T) {
	supported := map[string]string{
		".csv":     "csv",
		".json":    "json",
		".xlsx":    "excel",
		".xls":     "excel",
		".parquet": "parquet",
	}

	for ext, expected := range supported {
		got := getFileFormat("testfile" + ext)
		if got != expected {
			t.Errorf("getFileFormat(testfile%s) = %q, want %q", ext, got, expected)
		}
	}
}

func TestGetFileFormat_UnsupportedFormats(t *testing.T) {
	unsupported := []string{"data.txt", "data.xml", "data.yaml", "data.toml", "data.md", "data"}

	for _, path := range unsupported {
		if got := getFileFormat(path); got != "" {
			t.Errorf("getFileFormat(%q) = %q, expected empty string for unsupported format", path, got)
		}
	}
}

func TestRunClean_NoInputFile(t *testing.T) {
	err := runClean([]string{})
	if err == nil {
		t.Error("expected error when no input file is specified")
	}
	if !strings.Contains(err.Error(), "input file") {
		t.Errorf("error message should mention input file, got: %v", err)
	}
}

func TestRunClean_UnsupportedFormat(t *testing.T) {
	// Create a temp file with unsupported extension
	tmp, err := os.CreateTemp("", "test*.txt")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	tmp.Close()

	err = runClean([]string{tmp.Name()})
	if err == nil {
		t.Error("expected error for unsupported file format")
	}
	if !strings.Contains(err.Error(), "format") {
		t.Errorf("error message should mention format, got: %v", err)
	}
}

func TestRunClean_FileNotFound(t *testing.T) {
	err := runClean([]string{"nonexistent_file.csv"})
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestRunClean_TrimCSV(t *testing.T) {
	// Create a temp CSV file
	tmp, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	// Write test data
	tmp.WriteString("name,age\n  Alice  ,  30  \n  Bob  ,  25  \n")
	tmp.Close()

	// Determine output file
	outputFile := filepath.Join(os.TempDir(), "cleaned_test_output.csv")
	defer os.Remove(outputFile)

	err = runClean([]string{
		"-trim",
		"-output", outputFile,
		tmp.Name(),
	})
	if err != nil {
		t.Fatalf("runClean error: %v", err)
	}

	// Check output file exists
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}

func TestRunClean_NormalizeCase(t *testing.T) {
	tmp, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.WriteString("name,city\nalice,istanbul\nbob,ankara\n")
	tmp.Close()

	outputFile := filepath.Join(os.TempDir(), "cleaned_case_output.csv")
	defer os.Remove(outputFile)

	err = runClean([]string{
		"-case", "name:upper",
		"-output", outputFile,
		tmp.Name(),
	})
	if err != nil {
		t.Fatalf("runClean error: %v", err)
	}

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}

func TestRunClean_ReplaceNulls(t *testing.T) {
	tmp, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.WriteString("name,score\nAlice,\nBob,95\n")
	tmp.Close()

	outputFile := filepath.Join(os.TempDir(), "cleaned_nulls_output.csv")
	defer os.Remove(outputFile)

	err = runClean([]string{
		"-null-replace", "score:0",
		"-output", outputFile,
		tmp.Name(),
	})
	if err != nil {
		t.Fatalf("runClean error: %v", err)
	}

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}

func TestRunClean_FilterOutliers(t *testing.T) {
	tmp, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.WriteString("name,salary\nAlice,5000\nBob,99999\nCarol,4500\n")
	tmp.Close()

	outputFile := filepath.Join(os.TempDir(), "cleaned_outlier_output.csv")
	defer os.Remove(outputFile)

	err = runClean([]string{
		"-outlier", "salary:1000:10000",
		"-output", outputFile,
		tmp.Name(),
	})
	if err != nil {
		t.Fatalf("runClean error: %v", err)
	}

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}

func TestRunClean_ParallelTrim(t *testing.T) {
	tmp, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.WriteString("name,age\n  Alice  ,  30  \n")
	tmp.Close()

	outputFile := filepath.Join(os.TempDir(), "cleaned_parallel_output.csv")
	defer os.Remove(outputFile)

	err = runClean([]string{
		"-trim",
		"-parallel",
		"-workers", "2",
		"-output", outputFile,
		tmp.Name(),
	})
	if err != nil {
		t.Fatalf("runClean parallel error: %v", err)
	}

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}
