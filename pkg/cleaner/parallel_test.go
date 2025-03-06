package cleaner

import (
	"testing"
)

func TestTrimColumnsParallel(t *testing.T) {
	// Create a DataFrame for testing
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"  Ali  ", " 30 ", "  İstanbul  "},
		{"Ayşe  ", "25", "  Ankara"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Clean spaces in parallel
	trimmedDF := df.TrimColumnsParallel()
	trimmedData := trimmedDF.GetData()

	expectedData := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	for i, row := range expectedData {
		for j, expected := range row {
			if trimmedData[i][j] != expected {
				t.Errorf("TrimColumnsParallel()[%d][%d] = %v, beklenen = %v", i, j, trimmedData[i][j], expected)
			}
		}
	}
}

func TestReplaceNullsParallel(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "", "İstanbul"},
		{"", "25", "Ankara"},
		{"Mehmet", "NULL", ""},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Replace empty values in the Age column in parallel
	replacedDF, err := df.ReplaceNullsParallel("Age", "0")
	if err != nil {
		t.Fatalf("ReplaceNullsParallel error: %v", err)
	}

	replacedData := replacedDF.GetData()
	if replacedData[0][1] != "0" {
		t.Errorf("ReplaceNullsParallel('Age', '0')[0][1] = %v, expected = 0", replacedData[0][1])
	}

	// Replace empty values in the City column in parallel
	replacedDF, err = df.ReplaceNullsParallel("City", "Unknown")
	if err != nil {
		t.Fatalf("ReplaceNullsParallel error: %v", err)
	}

	replacedData = replacedDF.GetData()
	if replacedData[2][2] != "Unknown" {
		t.Errorf("ReplaceNullsParallel('City', 'Unknown')[2][2] = %v, expected = Unknown", replacedData[2][2])
	}

	// Check for non-existent column
	_, err = df.ReplaceNullsParallel("NonExistentColumn", "Value")
	if err == nil {
		t.Errorf("ReplaceNullsParallel('NonExistentColumn', 'Value') did not return an error, expected an error")
	}
}

func TestNormalizeCaseParallel(t *testing.T) {
	headers := []string{"Name", "City"}
	data := [][]string{
		{"ali", "istanbul"},
		{"AYŞE", "ANKARA"},
		{"Mehmet", "İzmir"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Convert to uppercase (parallel)
	upperDF, err := df.NormalizeCaseParallel("Name", true)
	if err != nil {
		t.Fatalf("NormalizeCaseParallel error: %v", err)
	}

	upperData := upperDF.GetData()
	expectedUpper := []string{"ALI", "AYŞE", "MEHMET"}

	for i, exp := range expectedUpper {
		if upperData[i][0] != exp {
			t.Errorf("NormalizeCaseParallel('Name', true)[%d][0] = %v, expected = %v",
				i, upperData[i][0], exp)
		}
	}

	// Convert to lowercase (parallel)
	lowerDF, err := df.NormalizeCaseParallel("City", false)
	if err != nil {
		t.Fatalf("NormalizeCaseParallel error: %v", err)
	}

	lowerData := lowerDF.GetData()
	expectedLower := []string{"istanbul", "ankara", "izmir"}

	for i, exp := range expectedLower {
		if lowerData[i][1] != exp {
			t.Errorf("NormalizeCaseParallel('City', false)[%d][1] = %v, expected = %v",
				i, lowerData[i][1], exp)
		}
	}
}

func TestCleanWithRegexParallel(t *testing.T) {
	headers := []string{"Name", "Phone", "Email"}
	data := [][]string{
		{"Ali", "555-123-4567", "ali@example.com"},
		{"Ayşe", "(555) 987-6543", "ayse@test.com"},
		{"Mehmet", "555.789.0123", "mehmet@domain.com"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Remove all special characters from phone numbers in parallel
	cleanedDF, err := df.CleanWithRegexParallel("Phone", "[^0-9]", "")
	if err != nil {
		t.Fatalf("CleanWithRegexParallel error: %v", err)
	}

	cleanedData := cleanedDF.GetData()
	expected := []string{"5551234567", "5559876543", "5557890123"}

	for i, exp := range expected {
		if cleanedData[i][1] != exp {
			t.Errorf("CleanWithRegexParallel('Phone', '[^0-9]', '')[%d][1] = %v, expected = %v",
				i, cleanedData[i][1], exp)
		}
	}
}

func TestFilterOutliersParallel(t *testing.T) {
	headers := []string{"Name", "Age", "Salary"}
	data := [][]string{
		{"Ali", "30", "5000"},
		{"Ayşe", "25", "4500"},
		{"Mehmet", "40", "15000"}, // Outlier
		{"Zeynep", "35", "6000"},
		{"Can", "28", "1000"}, // Aykırı değer
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Filter outliers in the Salary column in parallel (2000-10000 range)
	filteredDF, err := df.FilterOutliersParallel("Salary", 2000, 10000)
	if err != nil {
		t.Fatalf("FilterOutliersParallel error: %v", err)
	}

	filteredData := filteredDF.GetData()

	// After filtering, 3 rows should remain
	if len(filteredData) != 3 {
		t.Errorf("After filtering, the number of rows = %v, expected = 3", len(filteredData))
	}

	// Check the remaining data
	expectedNames := map[string]bool{
		"Ali":    true,
		"Ayşe":   true,
		"Zeynep": true,
	}

	for _, row := range filteredData {
		if !expectedNames[row[0]] {
			t.Errorf("After filtering, unexpected name: %s", row[0])
		}
	}
}

func TestCleanDatesParallel(t *testing.T) {
	headers := []string{"Name", "Birth Date"}
	data := [][]string{
		{"Ali", "1990-01-15"},
		{"Ayşe", "1995-05-20"},
		{"Mehmet", "2000-10-25"},
		{"Zeynep", "1985-03-01"},
		{"Can", "invalid-date"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Convert dates to standard format in parallel
	cleanedDF, err := df.CleanDatesParallel("Birth Date", "2006-01-02")
	if err != nil {
		t.Fatalf("CleanDatesParallel error: %v", err)
	}

	cleanedData := cleanedDF.GetData()

	// Check if valid dates are converted to the correct format
	expectedDates := map[string]string{
		"Ali":    "1990-01-15",
		"Ayşe":   "1995-05-20",
		"Mehmet": "2000-10-25",
		"Zeynep": "1985-03-01",
	}

	for _, row := range cleanedData {
		name := row[0]
		date := row[1]

		if expected, ok := expectedDates[name]; ok {
			if date != expected && name != "Can" { // Can's date is invalid
				t.Errorf("CleanDatesParallel('%s') = %v, expected = %v", name, date, expected)
			}
		}
	}

	// Check if the invalid date is kept
	found := false
	for _, row := range cleanedData {
		if row[0] == "Can" {
			found = true
			if row[1] != "invalid-date" {
				t.Errorf("Invalid date changed: %v, expected = invalid-date", row[1])
			}
			break
		}
	}

	if !found {
		t.Errorf("'Can' row not found")
	}

	// Check for non-existent column
	_, err = df.CleanDatesParallel("NonExistentColumn", "2006-01-02")
	if err == nil {
		t.Errorf("CleanDatesParallel('NonExistentColumn', ...) did not return an error, expected an error")
	}
}

func TestWithMaxWorkers(t *testing.T) {
	// Get default options
	opts := defaultParallelOptions()
	defaultWorkers := opts.MaxWorkers

	// Set a different number of workers
	newWorkers := defaultWorkers + 2
	option := WithMaxWorkers(newWorkers)

	// Apply the option
	option(opts)

	// Check if the number of workers has been updated
	if opts.MaxWorkers != newWorkers {
		t.Errorf("After WithMaxWorkers(%d), MaxWorkers = %d, expected = %d",
			newWorkers, opts.MaxWorkers, newWorkers)
	}

	// Check for negative value
	negativeWorkers := -1
	option = WithMaxWorkers(negativeWorkers)

	// Apply the option
	option(opts)

	// For negative value, the number of workers should not change
	if opts.MaxWorkers != newWorkers {
		t.Errorf("After WithMaxWorkers(%d), MaxWorkers changed, it should not have changed",
			negativeWorkers)
	}

	// Check for zero value
	zeroWorkers := 0
	option = WithMaxWorkers(zeroWorkers)

	// Apply the option
	option(opts)

	// For zero value, the number of workers should not change
	if opts.MaxWorkers != newWorkers {
		t.Errorf("After WithMaxWorkers(%d), MaxWorkers changed, it should not have changed",
			zeroWorkers)
	}
}
