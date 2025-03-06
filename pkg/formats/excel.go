package formats

import (
	"fmt"
	"strconv"

	"github.com/xuri/excelize/v2"
)

// ExcelOptions, Excel reading and writing options
type ExcelOptions struct {
	SheetName string // Sheet name
}

// ExcelOption, Excel options
type ExcelOption func(*ExcelOptions)

// defaultExcelOptions, default Excel options
func defaultExcelOptions() *ExcelOptions {
	return &ExcelOptions{
		SheetName: "Sheet1",
	}
}

// WithSheetName, Excel sheet name
func WithSheetName(sheetName string) ExcelOption {
	return func(o *ExcelOptions) {
		o.SheetName = sheetName
	}
}

// ReadExcelToRaw, read Excel file and return raw data
func ReadExcelToRaw(filePath string, options ...ExcelOption) ([]string, [][]string, error) {
	// Default options
	opts := defaultExcelOptions()

	// Apply user-specified options
	for _, option := range options {
		option(opts)
	}

	// Open Excel file
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("excel file cannot be opened: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing Excel file: %v\n", err)
		}
	}()

	// Check sheet
	sheetIndex, err := f.GetSheetIndex(opts.SheetName)
	if err != nil || sheetIndex == -1 {
		// Sheet not found, use first sheet
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return nil, nil, fmt.Errorf("sheet not found in excel file")
		}
		opts.SheetName = sheets[0]
	}

	// Read all rows
	rows, err := f.GetRows(opts.SheetName)
	if err != nil {
		return nil, nil, fmt.Errorf("excel rows cannot be read: %w", err)
	}

	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("excel file is empty")
	}

	// Get headers
	headers := rows[0]

	// Get data rows
	data := rows[1:]

	return headers, data, nil
}

// WriteExcelFromRaw, ham veriyi Excel dosyasına yazar
func WriteExcelFromRaw(headers []string, data [][]string, filePath string, options ...ExcelOption) error {
	// Default options
	opts := defaultExcelOptions()

	// Apply user-specified options
	for _, option := range options {
		option(opts)
	}

	// Create new Excel file
	f := excelize.NewFile()

	// Get default sheet
	defaultSheet := f.GetSheetName(0)

	// If default sheet name is different from requested sheet name, create new sheet
	if defaultSheet != opts.SheetName {
		_, err := f.NewSheet(opts.SheetName)
		if err != nil {
			return fmt.Errorf("new sheet cannot be created: %w", err)
		}
		// Delete default sheet
		f.DeleteSheet(defaultSheet)
	}

	// Write headers
	for i, header := range headers {
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return fmt.Errorf("cell coordinates cannot be calculated: %w", err)
		}
		f.SetCellValue(opts.SheetName, cell, header)
	}

	// Write data
	for i, row := range data {
		for j, value := range row {
			cell, err := excelize.CoordinatesToCellName(j+1, i+2) // i+2 because headers are in the first row
			if err != nil {
				return fmt.Errorf("cell coordinates cannot be calculated: %w", err)
			}

			// Save numeric values as numbers
			if isNumeric(value) {
				if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
					f.SetCellValue(opts.SheetName, cell, floatVal)
					continue
				}
			}

			// Save boolean values as booleans
			if value == "true" || value == "false" {
				if boolVal, err := strconv.ParseBool(value); err == nil {
					f.SetCellValue(opts.SheetName, cell, boolVal)
					continue
				}
			}

			// Save other values as strings
			f.SetCellValue(opts.SheetName, cell, value)
		}
	}

	// Save file
	if err := f.SaveAs(filePath); err != nil {
		return fmt.Errorf("excel file cannot be saved: %w", err)
	}

	return nil
}

// WriteExcel, Writes DataFrame to Excel file
func WriteExcel(df DataFrame, filePath string, options ...ExcelOption) error {
	return WriteExcelFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}

// isNumeric, a string's numeric or not
func isNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
