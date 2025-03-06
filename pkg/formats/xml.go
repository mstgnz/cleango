package formats

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
)

// XMLOptions contains XML reading and writing options
type XMLOptions struct {
	RootElement string // Root element name for XML
	ItemElement string // Item element name for XML
	Pretty      bool   // Format XML nicely
}

// XMLOption is a function type for setting XML options
type XMLOption func(*XMLOptions)

// defaultXMLOptions returns default XML options
func defaultXMLOptions() XMLOptions {
	return XMLOptions{
		RootElement: "root",
		ItemElement: "item",
		Pretty:      false,
	}
}

// WithXMLRootElement sets the root element name for XML
func WithXMLRootElement(rootElement string) XMLOption {
	return func(o *XMLOptions) {
		o.RootElement = rootElement
	}
}

// WithXMLItemElement sets the item element name for XML
func WithXMLItemElement(itemElement string) XMLOption {
	return func(o *XMLOptions) {
		o.ItemElement = itemElement
	}
}

// WithXMLPretty determines whether XML should be nicely formatted
func WithXMLPretty(pretty bool) XMLOption {
	return func(o *XMLOptions) {
		o.Pretty = pretty
	}
}

// ReadXMLToRaw reads an XML file and returns raw data
func ReadXMLToRaw(filePath string, options ...XMLOption) ([]string, [][]string, error) {
	// Default settings
	opts := defaultXMLOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open XML file: %w", err)
	}
	defer file.Close()

	// Parse XML
	decoder := xml.NewDecoder(file)

	var currentElement string
	var currentItem map[string]string
	var items []map[string]string
	var inRoot, inItem bool

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse XML: %w", err)
		}

		switch t := token.(type) {
		case xml.StartElement:
			if t.Name.Local == opts.RootElement {
				inRoot = true
				continue
			}

			if inRoot && t.Name.Local == opts.ItemElement {
				inItem = true
				currentItem = make(map[string]string)
				continue
			}

			if inItem {
				currentElement = t.Name.Local
			}

		case xml.EndElement:
			if t.Name.Local == opts.RootElement {
				inRoot = false
			}

			if t.Name.Local == opts.ItemElement {
				inItem = false
				items = append(items, currentItem)
				currentItem = nil
			}

			currentElement = ""

		case xml.CharData:
			if inItem && currentElement != "" {
				text := string(t)
				text = strings.TrimSpace(text)
				if text != "" {
					currentItem[currentElement] = text
				}
			}
		}
	}

	// Collect headers
	headers := make(map[string]bool)

	// Iterate through all records to collect unique headers
	for _, record := range items {
		for key := range record {
			headers[key] = true
		}
	}

	// Convert headers map to slice
	headerSlice := make([]string, 0, len(headers))
	for header := range headers {
		headerSlice = append(headerSlice, header)
	}

	// Convert data
	rows := make([][]string, len(items))
	for i, record := range items {
		row := make([]string, len(headerSlice))
		for j, header := range headerSlice {
			if val, ok := record[header]; ok {
				row[j] = val
			} else {
				row[j] = "" // Empty string for missing values
			}
		}
		rows[i] = row
	}

	return headerSlice, rows, nil
}

// WriteXMLFromRaw writes raw data to an XML file
func WriteXMLFromRaw(headers []string, data [][]string, filePath string, options ...XMLOption) error {
	// Default settings
	opts := defaultXMLOptions()

	// Apply user-specified settings
	for _, option := range options {
		option(&opts)
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create XML file: %w", err)
	}
	defer file.Close()

	// Create XML encoder
	encoder := xml.NewEncoder(file)
	if opts.Pretty {
		encoder.Indent("", "  ")
	}

	// Write XML header
	file.WriteString(xml.Header)

	// Start root element
	if err := encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: opts.RootElement}}); err != nil {
		return fmt.Errorf("failed to write XML root start element: %w", err)
	}

	// Write data
	for _, row := range data {
		// Start item element
		if err := encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: opts.ItemElement}}); err != nil {
			return fmt.Errorf("failed to write XML item start element: %w", err)
		}

		// Write fields
		for j, header := range headers {
			if j < len(row) && row[j] != "" {
				// Start field element
				if err := encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: header}}); err != nil {
					return fmt.Errorf("failed to write XML field start element: %w", err)
				}

				// Write field value
				if err := encoder.EncodeToken(xml.CharData(row[j])); err != nil {
					return fmt.Errorf("failed to write XML field value: %w", err)
				}

				// End field element
				if err := encoder.EncodeToken(xml.EndElement{Name: xml.Name{Local: header}}); err != nil {
					return fmt.Errorf("failed to write XML field end element: %w", err)
				}
			}
		}

		// End item element
		if err := encoder.EncodeToken(xml.EndElement{Name: xml.Name{Local: opts.ItemElement}}); err != nil {
			return fmt.Errorf("failed to write XML item end element: %w", err)
		}
	}

	// End root element
	if err := encoder.EncodeToken(xml.EndElement{Name: xml.Name{Local: opts.RootElement}}); err != nil {
		return fmt.Errorf("failed to write XML root end element: %w", err)
	}

	// Flush encoder
	if err := encoder.Flush(); err != nil {
		return fmt.Errorf("XML encoder error: %w", err)
	}

	return nil
}

// WriteXML writes DataFrame to an XML file
func WriteXML(df interface {
	GetHeaders() []string
	GetData() [][]string
}, filePath string, options ...XMLOption) error {
	return WriteXMLFromRaw(df.GetHeaders(), df.GetData(), filePath, options...)
}
