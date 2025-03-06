package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
)

// ReadCSV, CSV file is read and converted to DataFrame
func ReadCSV(filePath string, options ...formats.CSVOption) (*DataFrame, error) {
	headers, data, err := formats.ReadCSVToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteCSV, Writes DataFrame to CSV file
func (df *DataFrame) WriteCSV(filePath string, options ...formats.CSVOption) error {
	return formats.WriteCSV(df, filePath, options...)
}
