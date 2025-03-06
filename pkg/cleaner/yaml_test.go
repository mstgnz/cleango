package cleaner

import (
	"os"
	"testing"

	"github.com/mstgnz/cleango/pkg/formats"
)

func TestReadWriteYAML(t *testing.T) {
	// Create a temporary YAML file
	tempFile, err := os.CreateTemp("", "test_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFileName)

	// Create test data
	yamlContent := `- id: 1
  name: John Doe
  email: john@example.com
  age: 30
  created_at: 2023-01-15
  active: true
  score: 85.5
- id: 2
  name: Jane Smith
  email: jane@example.com
  age: 25
  created_at: 2023-02-20
  active: true
  score: 92.0
- id: 3
  name: Bob Johnson
  email: bob@example.com
  age: 40
  created_at: 2023-03-10
  active: false
  score: 78.3`

	// Write test data to file
	err = os.WriteFile(tempFileName, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Read YAML file
	df, err := ReadYAML(tempFileName)
	if err != nil {
		t.Fatalf("Failed to read YAML: %v", err)
	}

	// Check data
	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("Expected 3 rows, got %d", rows)
	}
	if cols < 7 {
		t.Errorf("Expected at least 7 columns, got %d", cols)
	}

	// Create a temporary output file
	outFile, err := os.CreateTemp("", "test_out_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	outFileName := outFile.Name()
	outFile.Close()
	defer os.Remove(outFileName)

	// Write DataFrame to YAML
	err = df.WriteYAML(outFileName, formats.WithYAMLPretty(true))
	if err != nil {
		t.Fatalf("Failed to write YAML: %v", err)
	}

	// Read the written file
	df2, err := ReadYAML(outFileName)
	if err != nil {
		t.Fatalf("Failed to read written YAML: %v", err)
	}

	// Check data
	rows2, cols2 := df2.Shape()
	if rows2 != rows {
		t.Errorf("Row count mismatch: got %d, want %d", rows2, rows)
	}
	if cols2 != cols {
		t.Errorf("Column count mismatch: got %d, want %d", cols2, cols)
	}
}
