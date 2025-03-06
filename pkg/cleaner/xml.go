package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
)

// ReadXML reads an XML file and returns a DataFrame
func ReadXML(filePath string, options ...formats.XMLOption) (*DataFrame, error) {
	headers, data, err := formats.ReadXMLToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteXML writes a DataFrame to an XML file
func (df *DataFrame) WriteXML(filePath string, options ...formats.XMLOption) error {
	return formats.WriteXML(df, filePath, options...)
}
