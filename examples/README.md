# CleanGo Examples

This directory contains example files demonstrating the usage of the CleanGo library.

## Example Data Files

- `sample_data.csv`: Example data in CSV format
- `sample_data.json`: Example data in JSON format
- `sample_data.xml`: Example data in XML format
- `sample_data.xlsx`: Example data in Excel format
- `sample_data.parquet`: Example data in Parquet format
- `api_request.json`: Example JSON request for API
- `sample_data.csv`: CSV sample data in format
- `sample_data.json`: JSON sample data in format
- `sample_data.xlsx`: Excel sample data in format
- `sample_data.parquet`: Parquet sample data in format
- `api_request.json`: Example JSON request for API

## Example Usages

### CLI Usage

CSV file cleaning (basic):

```bash
cleango clean examples/sample_data.csv --trim --date-format="created_at:2006-01-02" --output=cleaned.csv
```

JSON file cleaning:

```bash
cleango clean examples/sample_data.json --trim --date-format="created_at:2006-01-02" --output=cleaned.json
```

XML file cleaning:

```bash
cleango clean examples/sample_data.xml --trim --date-format="created_at:2006-01-02" --root-element="root" --item-element="item" --output=cleaned.xml
```

Excel file cleaning:

```bash
cleango clean examples/sample_data.xlsx --trim --date-format="created_at:2006-01-02" --sheet-name="Sheet1" --output=cleaned.xlsx
```

Parquet file cleaning:

```bash
cleango clean examples/sample_data.parquet --trim --date-format="created_at:2006-01-02" --compression=snappy --output=cleaned.parquet
```

Reading CSV file and saving in different formats:

```bash
cleango clean examples/sample_data.csv --trim --format=json --output=cleaned.json
cleango clean examples/sample_data.csv --trim --format=xml --output=cleaned.xml
cleango clean examples/sample_data.csv --trim --format=excel --output=cleaned.xlsx
cleango clean examples/sample_data.csv --trim --format=parquet --output=cleaned.parquet
```

Advanced cleaning operations:

```bash
cleango clean examples/sample_data.csv \
  --trim \
  --date-format="created_at:2006-01-02" \
  --null-replace="age:0" \
  --case="name:lower,email:lower" \
  --regex="email:@example\.com:@cleango.org" \
  --split="full_name: :first_name,last_name" \
  --outlier="age:18:65" \
  --output=cleaned.csv
```

Cleaning with parallel processing:

```bash
cleango clean examples/sample_data.csv \
  --trim \
  --date-format="created_at:2006-01-02" \
  --null-replace="age:0" \
  --case="name:lower,email:lower" \
  --regex="email:@example\.com:@cleango.org" \
  --outlier="age:18:65" \
  --parallel \
  --workers=8 \
  --output=cleaned.csv
```

### API Usage

Starting the API:

```bash
go run cmd/api/main.go
```

Sending request to API (data cleaning):

```bash
curl -X POST -H "Content-Type: application/json" -d @examples/api_request.json http://localhost:8080/clean
```

Sending request to API (parallel data cleaning):

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "data": [{"name": "John Doe", "email": "john@example.com"}],
  "actions": ["trim", "normalize_case:name=lower"],
  "parallel": true,
  "max_workers": 4
}' http://localhost:8080/clean
```

Sending request to API (file cleaning):

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "file_path": "examples/sample_data.csv",
  "actions": ["trim", "normalize_dates:created_at=2006-01-02"],
  "format": "json",
  "output": "cleaned.json"
}' http://localhost:8080/clean-file
```

Sending request to API (parallel file cleaning):

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "file_path": "examples/sample_data.csv",
  "actions": ["trim", "normalize_dates:created_at=2006-01-02"],
  "format": "json",
  "output": "cleaned.json",
  "parallel": true,
  "max_workers": 8
}' http://localhost:8080/clean-file
```

### Usage as Go Library

```go
package main

import (
	"fmt"
	"log"

	"github.com/mstgnz/cleango/pkg/cleaner"
	"github.com/mstgnz/cleango/pkg/formats"
	"github.com/xitongsys/parquet-go/parquet"
)

func main() {
	// Read CSV file
	df, err := cleaner.ReadCSV("examples/sample_data.csv")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Read XML file
	xmlDf, err := cleaner.ReadXML("examples/sample_data.xml",
		formats.WithXMLRootElement("root"),
		formats.WithXMLItemElement("item"))
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Basic cleaning operations
	df.TrimColumns()
	df.CleanDates("created_at", "2006-01-02")
	df.ReplaceNulls("age", "0")
	df.NormalizeCase("name", false) // lowercase

	// Advanced cleaning operations
	df.CleanWithRegex("email", "@example\\.com", "@cleango.org")
	df.SplitColumn("full_name", " ", []string{"first_name", "last_name"})
	df.FilterOutliers("age", 18, 65)

	// Save in different formats
	df.WriteCSV("cleaned.csv")
	df.WriteJSON("cleaned.json")
	df.WriteXML("cleaned.xml", formats.WithXMLPretty(true), formats.WithXMLRootElement("users"), formats.WithXMLItemElement("user"))
	df.WriteExcel("cleaned.xlsx", formats.WithSheetName("Temizlenmiş Veri"))
	df.WriteParquet("cleaned.parquet", cleaner.WithParquetCompression(parquet.CompressionCodec_SNAPPY))

	// Show statistics
	rows, cols := df.Shape()
	fmt.Printf("Row count: %d, Column count: %d\n", rows, cols)
}
```
