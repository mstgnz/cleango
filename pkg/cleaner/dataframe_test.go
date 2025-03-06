package cleaner

import (
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	tests := []struct {
		name    string
		headers []string
		data    [][]string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Valid DataFrame",
			headers: []string{"Name", "Age", "City"},
			data: [][]string{
				{"Ali", "30", "İstanbul"},
				{"Ayşe", "25", "Ankara"},
				{"Mehmet", "40", "İzmir"},
			},
			wantErr: false,
		},
		{
			name:    "Empty Headers",
			headers: []string{},
			data:    [][]string{{"Ali", "30", "İstanbul"}},
			wantErr: true,
			errMsg:  "headers cannot be empty",
		},
		{
			name:    "Incompatible Column Count",
			headers: []string{"Name", "Age", "City"},
			data: [][]string{
				{"Ali", "30", "İstanbul"},
				{"Ayşe", "25"}, // Missing column
			},
			wantErr: true,
			errMsg:  "row 1 has an incompatible number of columns: 2 (expected: 3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df, err := NewDataFrame(tt.headers, tt.data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewDataFrame() expected error, but no error occurred")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("NewDataFrame() error message = %v, expected = %v", err.Error(), tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("NewDataFrame() unexpected error = %v", err)
				return
			}

			if len(df.Headers) != len(tt.headers) {
				t.Errorf("Headers length = %v, expected = %v", len(df.Headers), len(tt.headers))
			}

			if len(df.Data) != len(tt.data) {
				t.Errorf("Data length = %v, expected = %v", len(df.Data), len(tt.data))
			}
		})
	}
}

func TestGetHeaders(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	gotHeaders := df.GetHeaders()
	if len(gotHeaders) != len(headers) {
		t.Errorf("GetHeaders() length = %v, expected = %v", len(gotHeaders), len(headers))
	}

	for i, h := range headers {
		if gotHeaders[i] != h {
			t.Errorf("GetHeaders()[%d] = %v, expected = %v", i, gotHeaders[i], h)
		}
	}
}

func TestGetData(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	gotData := df.GetData()
	if len(gotData) != len(data) {
		t.Errorf("GetData() row count = %v, expected = %v", len(gotData), len(data))
	}

	for i, row := range data {
		if len(gotData[i]) != len(row) {
			t.Errorf("GetData()[%d] column count = %v, expected = %v", i, len(gotData[i]), len(row))
			continue
		}

		for j, cell := range row {
			if gotData[i][j] != cell {
				t.Errorf("GetData()[%d][%d] = %v, expected = %v", i, j, gotData[i][j], cell)
			}
		}
	}
}

func TestTrimColumns(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"  Ali  ", " 30 ", "  İstanbul  "},
		{"Ayşe  ", "25", "  Ankara"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	trimmedDF := df.TrimColumns()
	trimmedData := trimmedDF.GetData()

	expectedData := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	for i, row := range expectedData {
		for j, expected := range row {
			if trimmedData[i][j] != expected {
				t.Errorf("TrimColumns()[%d][%d] = %v, expected = %v", i, j, trimmedData[i][j], expected)
			}
		}
	}
}

func TestReplaceNulls(t *testing.T) {
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

	// Replace empty values in the Age column
	replacedDF, err := df.ReplaceNulls("Age", "0")
	if err != nil {
		t.Fatalf("ReplaceNulls error: %v", err)
	}

	replacedData := replacedDF.GetData()
	if replacedData[0][1] != "0" {
		t.Errorf("ReplaceNulls('Age', '0')[0][1] = %v, expected = 0", replacedData[0][1])
	}

	// Replace empty values in the City column
	replacedDF, err = df.ReplaceNulls("City", "Unknown")
	if err != nil {
		t.Fatalf("ReplaceNulls error: %v", err)
	}

	replacedData = replacedDF.GetData()
	if replacedData[2][2] != "Unknown" {
		t.Errorf("ReplaceNulls('City', 'Unknown')[2][2] = %v, expected = Unknown", replacedData[2][2])
	}

	// Check for non-existent column
	_, err = df.ReplaceNulls("Non-Column", "Value")
	if err == nil {
		t.Errorf("ReplaceNulls('Non-Column', 'Value') expected error, but no error occurred")
	}
}

func TestRenameColumn(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Valid column rename
	renamedDF, err := df.RenameColumn("Age", "Years")
	if err != nil {
		t.Fatalf("RenameColumn error: %v", err)
	}

	renamedHeaders := renamedDF.GetHeaders()
	found := false
	for _, h := range renamedHeaders {
		if h == "Years" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("RenameColumn('Age', 'Years') after 'Years' header not found")
	}

	// Check for non-existent column
	_, err = df.RenameColumn("Non-Column", "NewName")
	if err == nil {
		t.Errorf("RenameColumn('Non-Column', 'NewName') expected error, but no error occurred")
	}
}

func TestCleanWithRegex(t *testing.T) {
	headers := []string{"Name", "Phone", "Email"}
	data := [][]string{
		{"Ali", "555-123-4567", "ali@example.com"},
		{"Ayşe", "(555) 987-6543", "ayse@test.com"},
		{"Mehmet", "555.789.0123", "mehmet@domain.com"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Remove all special characters from phone numbers
	cleanedDF, err := df.CleanWithRegex("Phone", "[^0-9]", "")
	if err != nil {
		t.Fatalf("CleanWithRegex error: %v", err)
	}

	cleanedData := cleanedDF.GetData()
	expected := []string{"5551234567", "5559876543", "5557890123"}

	for i, exp := range expected {
		if cleanedData[i][1] != exp {
			t.Errorf("CleanWithRegex('Phone', '[^0-9]', '')[%d][1] = %v, expected = %v",
				i, cleanedData[i][1], exp)
		}
	}

	// Check for non-existent column
	_, err = df.CleanWithRegex("Non-Column", "[^0-9]", "")
	if err == nil {
		t.Errorf("CleanWithRegex('Non-Column', '[^0-9]', '') expected error, but no error occurred")
	}
}

func TestNormalizeCase(t *testing.T) {
	headers := []string{"Name", "City"}
	data := [][]string{
		{"ali", "istanbul"},
		{"AYŞE", "ANKARA"},
		{"Mehmet", "İzmir"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Convert to uppercase
	upperDF, err := df.NormalizeCase("Name", true)
	if err != nil {
		t.Fatalf("NormalizeCase error: %v", err)
	}

	upperData := upperDF.GetData()
	expectedUpper := []string{"ALI", "AYŞE", "MEHMET"}

	for i, exp := range expectedUpper {
		if upperData[i][0] != exp {
			t.Errorf("NormalizeCase('Name', true)[%d][0] = %v, expected = %v",
				i, upperData[i][0], exp)
		}
	}

	// Convert to lowercase
	lowerDF, err := df.NormalizeCase("City", false)
	if err != nil {
		t.Fatalf("NormalizeCase error: %v", err)
	}

	lowerData := lowerDF.GetData()
	expectedLower := []string{"istanbul", "ankara", "izmir"}

	for i, exp := range expectedLower {
		if lowerData[i][1] != exp {
			t.Errorf("NormalizeCase('City', false)[%d][1] = %v, expected = %v",
				i, lowerData[i][1], exp)
		}
	}

	// Check for non-existent column
	_, err = df.NormalizeCase("Non-Column", true)
	if err == nil {
		t.Errorf("NormalizeCase('Non-Column', true) expected error, but no error occurred")
	}
}

func TestSplitColumn(t *testing.T) {
	headers := []string{"ID", "FullName"}
	data := [][]string{
		{"1", "Ali Yılmaz"},
		{"2", "Ayşe Kaya"},
		{"3", "Mehmet Can Demir"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Split the FullName column by spaces (use unique column names)
	splitDF, err := df.SplitColumn("FullName", " ", []string{"FirstName", "LastName", "SecondLastName"})
	if err != nil {
		t.Fatalf("SplitColumn error: %v", err)
	}

	splitHeaders := splitDF.GetHeaders()
	// New columns are added instead of the FullName column
	expectedHeaders := []string{"FirstName", "LastName", "SecondLastName"}

	// Check if headers are correct
	for _, expected := range expectedHeaders {
		found := false
		for _, h := range splitHeaders {
			if h == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("After SplitColumn, '%s' header not found", expected)
		}
	}

	// Check if data is split correctly
	splitData := splitDF.GetData()

	// Find the index of the new columns
	firstNameIndex := -1
	lastNameIndex := -1
	secondLastNameIndex := -1

	for i, h := range splitHeaders {
		if h == "FirstName" {
			firstNameIndex = i
		} else if h == "LastName" {
			lastNameIndex = i
		} else if h == "SecondLastName" {
			secondLastNameIndex = i
		}
	}

	if firstNameIndex == -1 || lastNameIndex == -1 || secondLastNameIndex == -1 {
		t.Fatalf("Split columns not found")
	}

	// Ali Yılmaz
	if splitData[0][firstNameIndex] != "Ali" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[0][FirstName] = %v, expected = Ali", splitData[0][firstNameIndex])
	}
	if splitData[0][lastNameIndex] != "Yılmaz" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[0][LastName] = %v, expected = Yılmaz", splitData[0][lastNameIndex])
	}
	if splitData[0][secondLastNameIndex] != "" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[0][SecondLastName] = %v, expected = ''", splitData[0][secondLastNameIndex])
	}

	// Mehmet Can Demir
	if splitData[2][firstNameIndex] != "Mehmet" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[2][FirstName] = %v, expected = Mehmet", splitData[2][firstNameIndex])
	}
	if splitData[2][lastNameIndex] != "Can" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[2][LastName] = %v, expected = Can", splitData[2][lastNameIndex])
	}
	if splitData[2][secondLastNameIndex] != "Demir" {
		t.Errorf("SplitColumn('FullName', ' ', ...)[2][SecondLastName] = %v, expected = Demir", splitData[2][secondLastNameIndex])
	}

	// Check for non-existent column
	_, err = df.SplitColumn("Non-Column", " ", []string{"New1", "New2"})
	if err == nil {
		t.Errorf("SplitColumn('Non-Column', ...) expected error, but no error occurred")
	}
}

func TestShape(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
		{"Mehmet", "40", "İzmir"},
		{"Zeynep", "35", "Bursa"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	rows, cols := df.Shape()

	if rows != 4 {
		t.Errorf("Shape() row count = %v, expected = 4", rows)
	}

	if cols != 3 {
		t.Errorf("Shape() column count = %v, expected = 3", cols)
	}
}

func TestHead(t *testing.T) {
	headers := []string{"Name", "Age", "City"}
	data := [][]string{
		{"Ali", "30", "İstanbul"},
		{"Ayşe", "25", "Ankara"},
		{"Mehmet", "40", "İzmir"},
		{"Zeynep", "35", "Bursa"},
		{"Can", "28", "Antalya"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// İlk 3 satırı al (başlıklar dahil)
	head := df.Head(3)

	// Başlıklar dahil 4 satır olmalı (başlık + 3 veri satırı)
	if len(head) != 4 {
		t.Errorf("Head(3) row count = %v, expected = 4", len(head))
	}

	// İlk satır başlıklar olmalı
	for i, h := range headers {
		if head[0][i] != h {
			t.Errorf("Head(3)[0][%d] = %v, expected = %v", i, head[0][i], h)
		}
	}

	// Veri içeriğini kontrol et
	for i := 0; i < 3; i++ {
		for j := 0; j < len(headers); j++ {
			if head[i+1][j] != data[i][j] {
				t.Errorf("Head(3)[%d][%d] = %v, expected = %v", i+1, j, head[i+1][j], data[i][j])
			}
		}
	}

	// Veri sayısından fazla isteme durumu
	head = df.Head(10)
	// Başlıklar dahil 6 satır olmalı (başlık + 5 veri satırı)
	if len(head) != 6 {
		t.Errorf("Head(10) row count = %v, expected = 6", len(head))
	}
}

func TestFilterOutliers(t *testing.T) {
	headers := []string{"Name", "Age", "Salary"}
	data := [][]string{
		{"Ali", "30", "5000"},
		{"Ayşe", "25", "4500"},
		{"Mehmet", "40", "15000"}, // Outlier
		{"Zeynep", "35", "6000"},
		{"Can", "28", "1000"}, // Outlier
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Filter outliers in the Salary column (2000-10000 range)
	filteredDF, err := df.FilterOutliers("Salary", 2000, 10000)
	if err != nil {
		t.Fatalf("FilterOutliers error: %v", err)
	}

	filteredData := filteredDF.GetData()

	// After filtering, 3 rows should remain
	if len(filteredData) != 3 {
		t.Errorf("After FilterOutliers, row count = %v, expected = 3", len(filteredData))
	}

	// Check the remaining data
	expectedNames := map[string]bool{
		"Ali":    true,
		"Ayşe":   true,
		"Zeynep": true,
	}

	for _, row := range filteredData {
		if !expectedNames[row[0]] {
			t.Errorf("After FilterOutliers, unexpected name: %s", row[0])
		}
	}

	// Check for non-numeric column
	_, err = df.FilterOutliers("Name", 0, 100)
	if err == nil {
		t.Errorf("FilterOutliers('Name', 0, 100) expected error, but no error occurred")
	}

	// Check for non-existent column
	_, err = df.FilterOutliers("Non-Column", 0, 100)
	if err == nil {
		t.Errorf("FilterOutliers('Non-Column', 0, 100) expected error, but no error occurred")
	}
}

func TestCleanDates(t *testing.T) {
	headers := []string{"Name", "BirthDate"}
	data := [][]string{
		{"Ali", "1990-01-15"},
		{"Ayşe", "1995-05-20"},
		{"Mehmet", "2000-10-25"},
		{"Zeynep", "1985-03-01"},
	}

	df, err := NewDataFrame(headers, data)
	if err != nil {
		t.Fatalf("DataFrame creation failed: %v", err)
	}

	// Convert dates to standard format
	cleanedDF, err := df.CleanDates("BirthDate", "2006-01-02")
	if err != nil {
		t.Fatalf("CleanDates error: %v", err)
	}

	cleanedData := cleanedDF.GetData()

	// Check that valid dates have been converted to the correct format
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
			if date != expected {
				t.Errorf("CleanDates('%s') = %v, expected = %v", name, date, expected)
			}
		}
	}

	// Error checking for nonexistent column
	_, err = df.CleanDates("Non-Column", "2006-01-02")
	if err == nil {
		t.Errorf("CleanDates('Non-Column', ...) expected error, but no error occurred")
	}
}
