package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
)

// ReadExcel, Excel file is read and converted to DataFrame
func ReadExcel(filePath string, options ...formats.ExcelOption) (*DataFrame, error) {
	headers, data, err := formats.ReadExcelToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteExcel, DataFrame is written to Excel file
func (df *DataFrame) WriteExcel(filePath string, options ...formats.ExcelOption) error {
	return formats.WriteExcel(df, filePath, options...)
}
