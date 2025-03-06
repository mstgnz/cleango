package cleaner

import (
	"os"
	"testing"

	"github.com/mstgnz/cleango/pkg/formats"
)

func TestReadWriteXML(t *testing.T) {
	// Create a temporary XML file
	tempFile, err := os.CreateTemp("", "test_*.xml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempFileName)

	// Create test data
	xmlContent := `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <item>
    <id>1</id>
    <name>John Doe</name>
    <email>john@example.com</email>
    <age>30</age>
  </item>
  <item>
    <id>2</id>
    <name>Jane Smith</name>
    <email>jane@example.com</email>
    <age>25</age>
  </item>
  <item>
    <id>3</id>
    <name>Bob Johnson</name>
    <email>bob@example.com</email>
    <age>40</age>
  </item>
</root>`

	// Write test data to file
	err = os.WriteFile(tempFileName, []byte(xmlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Read XML file
	df, err := ReadXML(tempFileName)
	if err != nil {
		t.Fatalf("Failed to read XML: %v", err)
	}

	// Check data
	if len(df.Headers) != 4 {
		t.Errorf("Expected 4 headers, got %d", len(df.Headers))
	}

	if len(df.Data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(df.Data))
	}

	// Create a temporary output file
	outFile, err := os.CreateTemp("", "test_out_*.xml")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	outFileName := outFile.Name()
	outFile.Close()
	defer os.Remove(outFileName)

	// Write DataFrame to XML
	err = df.WriteXML(outFileName, formats.WithXMLPretty(true))
	if err != nil {
		t.Fatalf("Failed to write XML: %v", err)
	}

	// Read the written file
	df2, err := ReadXML(outFileName)
	if err != nil {
		t.Fatalf("Failed to read written XML: %v", err)
	}

	// Check data
	if len(df2.Headers) != len(df.Headers) {
		t.Errorf("Header count mismatch: got %d, want %d", len(df2.Headers), len(df.Headers))
	}

	if len(df2.Data) != len(df.Data) {
		t.Errorf("Row count mismatch: got %d, want %d", len(df2.Data), len(df.Data))
	}

	// Test with custom root and item elements
	outFile2, err := os.CreateTemp("", "test_out2_*.xml")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	outFileName2 := outFile2.Name()
	outFile2.Close()
	defer os.Remove(outFileName2)

	// Write DataFrame to XML with custom elements
	err = df.WriteXML(outFileName2,
		formats.WithXMLRootElement("users"),
		formats.WithXMLItemElement("user"),
		formats.WithXMLPretty(true))
	if err != nil {
		t.Fatalf("Failed to write XML: %v", err)
	}

	// Read the written file with custom elements
	df3, err := ReadXML(outFileName2,
		formats.WithXMLRootElement("users"),
		formats.WithXMLItemElement("user"))
	if err != nil {
		t.Fatalf("Failed to read written XML: %v", err)
	}

	// Check data
	if len(df3.Headers) != len(df.Headers) {
		t.Errorf("Header count mismatch: got %d, want %d", len(df3.Headers), len(df.Headers))
	}

	if len(df3.Data) != len(df.Data) {
		t.Errorf("Row count mismatch: got %d, want %d", len(df3.Data), len(df.Data))
	}
}
