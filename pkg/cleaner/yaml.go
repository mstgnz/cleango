package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
)

// ReadYAML reads a YAML file and returns a DataFrame
func ReadYAML(filePath string, options ...formats.YAMLOption) (*DataFrame, error) {
	headers, data, err := formats.ReadYAMLToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteYAML writes a DataFrame to a YAML file
func (df *DataFrame) WriteYAML(filePath string, options ...formats.YAMLOption) error {
	return formats.WriteYAML(df, filePath, options...)
}
