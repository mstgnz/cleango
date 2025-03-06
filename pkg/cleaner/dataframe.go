package cleaner

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DataFrame is the basic data structure for data cleaning operations
type DataFrame struct {
	Headers []string        // Column headers
	Data    [][]string      // Data consisting of rows and columns
	Types   map[string]Type // Data type of each column
}

// GetHeaders returns the headers of the DataFrame
func (df *DataFrame) GetHeaders() []string {
	return df.Headers
}

// GetData, Returns the data of the DataFrame
func (df *DataFrame) GetData() [][]string {
	return df.Data
}

// Type, columns data type
type Type int

const (
	TypeString Type = iota
	TypeInt
	TypeFloat
	TypeDate
	TypeBool
	TypeJSON
)

// NewDataFrame, new DataFrame
func NewDataFrame(headers []string, data [][]string) (*DataFrame, error) {
	if len(headers) == 0 {
		return nil, errors.New("headers cannot be empty")
	}

	// Check if all rows have the same number of columns
	for i, row := range data {
		if len(row) != len(headers) {
			return nil, fmt.Errorf("row %d has an incompatible number of columns: %d (expected: %d)", i, len(row), len(headers))
		}
	}

	// Automatically determine column types
	types := make(map[string]Type)
	for _, header := range headers {
		types[header] = TypeString // Default to string
	}

	return &DataFrame{
		Headers: headers,
		Data:    data,
		Types:   types,
	}, nil
}

// TrimColumns, all column values's leading and trailing spaces
func (df *DataFrame) TrimColumns() *DataFrame {
	for i := range df.Data {
		for j := range df.Data[i] {
			df.Data[i][j] = strings.TrimSpace(df.Data[i][j])
		}
	}
	return df
}

// ReplaceNulls, replace empty values with the specified default value
func (df *DataFrame) ReplaceNulls(column string, defaultValue string) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	for i := range df.Data {
		if df.Data[i][colIndex] == "" {
			df.Data[i][colIndex] = defaultValue
		}
	}
	return df, nil
}

// CleanDates, convert date values in the specified column to the specified format
func (df *DataFrame) CleanDates(column string, layout string) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	for i := range df.Data {
		if df.Data[i][colIndex] == "" {
			continue
		}

		// Parse date and convert to specified format
		t, err := time.Parse(time.RFC3339, df.Data[i][colIndex])
		if err != nil {
			// Try different formats
			t, err = time.Parse("2006-01-02", df.Data[i][colIndex])
			if err != nil {
				t, err = time.Parse("02/01/2006", df.Data[i][colIndex])
				if err != nil {
					t, err = time.Parse("01/02/2006", df.Data[i][colIndex])
					if err != nil {
						return nil, fmt.Errorf("row %d, column %s: date format not found: %s", i, column, df.Data[i][colIndex])
					}
				}
			}
		}

		// Convert date to specified format
		df.Data[i][colIndex] = t.Format(layout)
	}

	df.Types[column] = TypeDate
	return df, nil
}

// NormalizeCase, convert the values in the specified column to uppercase or lowercase
func (df *DataFrame) NormalizeCase(column string, toUpper bool) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	for i := range df.Data {
		if toUpper {
			df.Data[i][colIndex] = strings.ToUpper(df.Data[i][colIndex])
		} else {
			df.Data[i][colIndex] = strings.ToLower(df.Data[i][colIndex])
		}
	}
	return df, nil
}

// RenameColumn, rename a column
func (df *DataFrame) RenameColumn(oldName, newName string) (*DataFrame, error) {
	colIndex := df.getColumnIndex(oldName)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", oldName)
	}

	// If the new name already exists, return an error
	if df.getColumnIndex(newName) != -1 {
		return nil, fmt.Errorf("column already exists: %s", newName)
	}

	// Update the header
	df.Headers[colIndex] = newName

	// Update the type
	if t, ok := df.Types[oldName]; ok {
		df.Types[newName] = t
		delete(df.Types, oldName)
	}

	return df, nil
}

// getColumnIndex, return the index of the specified column
func (df *DataFrame) getColumnIndex(column string) int {
	for i, header := range df.Headers {
		if header == column {
			return i
		}
	}
	return -1
}

// Head, return the first n rows of the DataFrame
func (df *DataFrame) Head(n int) [][]string {
	if n <= 0 {
		return [][]string{}
	}
	if n > len(df.Data) {
		n = len(df.Data)
	}

	result := make([][]string, n+1)
	result[0] = df.Headers
	for i := 0; i < n; i++ {
		result[i+1] = df.Data[i]
	}
	return result
}

// Shape, return the size of the DataFrame (row count, column count)
func (df *DataFrame) Shape() (int, int) {
	return len(df.Data), len(df.Headers)
}

// CleanWithRegex, clean the values in the specified column with regex
func (df *DataFrame) CleanWithRegex(column string, pattern string, replacement string) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	// Compile regex
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}

	// Clean the values
	for i := range df.Data {
		df.Data[i][colIndex] = re.ReplaceAllString(df.Data[i][colIndex], replacement)
	}

	return df, nil
}

// SplitColumn, split a column by the specified separator and create new columns
func (df *DataFrame) SplitColumn(column string, separator string, newColumns []string) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	// Yeni sütun sayısı kontrol et
	if len(newColumns) == 0 {
		return nil, errors.New("at least one new column name must be specified")
	}

	// Yeni sütun adlarının benzersiz olduğunu kontrol et
	for _, newCol := range newColumns {
		if df.getColumnIndex(newCol) != -1 {
			return nil, fmt.Errorf("column already exists: %s", newCol)
		}
	}

	// Update the headers
	newHeaders := make([]string, 0, len(df.Headers)+len(newColumns)-1)
	for i, header := range df.Headers {
		if i == colIndex {
			// Add new columns instead of the original column
			newHeaders = append(newHeaders, newColumns...)
		} else {
			newHeaders = append(newHeaders, header)
		}
	}

	// Update the data
	newData := make([][]string, len(df.Data))
	for i, row := range df.Data {
		// Split the column
		parts := strings.Split(row[colIndex], separator)

		// Create a new row
		newRow := make([]string, 0, len(row)+len(newColumns)-1)
		for j, cell := range row {
			if j == colIndex {
				// Add the split values
				for k := 0; k < len(newColumns); k++ {
					if k < len(parts) {
						newRow = append(newRow, parts[k])
					} else {
						newRow = append(newRow, "") // For missing values, empty string
					}
				}
			} else {
				newRow = append(newRow, cell)
			}
		}
		newData[i] = newRow
	}

	// Update the type
	newTypes := make(map[string]Type)
	for header, typ := range df.Types {
		if header != column {
			newTypes[header] = typ
		}
	}
	// For new columns, default type
	for _, newCol := range newColumns {
		newTypes[newCol] = TypeString
	}

	// Create a new DataFrame
	df.Headers = newHeaders
	df.Data = newData
	df.Types = newTypes

	return df, nil
}

// FilterOutliers, filter the outliers in the specified numerical column
func (df *DataFrame) FilterOutliers(column string, min, max float64) (*DataFrame, error) {
	colIndex := df.getColumnIndex(column)
	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", column)
	}

	// Collect filtered data
	var filteredData [][]string
	for _, row := range df.Data {
		// Skip empty values
		if row[colIndex] == "" {
			filteredData = append(filteredData, row)
			continue
		}

		// Convert to number
		val, err := strconv.ParseFloat(row[colIndex], 64)
		if err != nil {
			return nil, fmt.Errorf("conversion error: %w", err)
		}

		// Check if it is within the range
		if val >= min && val <= max {
			filteredData = append(filteredData, row)
		}
	}

	// Update the DataFrame
	df.Data = filteredData

	return df, nil
}
