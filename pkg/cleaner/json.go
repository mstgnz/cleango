package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
)

// ReadJSON reads a JSON file and converts it to DataFrame
func ReadJSON(filePath string, options ...formats.JSONOption) (*DataFrame, error) {
	headers, data, err := formats.ReadJSONToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteJSON writes DataFrame to a JSON file
func (df *DataFrame) WriteJSON(filePath string, options ...formats.JSONOption) error {
	return formats.WriteJSON(df, filePath, options...)
}
