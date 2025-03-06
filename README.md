# CleanGo - Data Cleaning at Go Speed 🚀

CleanGo is a library that performs data cleaning and transformation operations with the speed and efficiency of the Go language.

## Features

- ✅ Reading and writing data in CSV, JSON, Excel, and Parquet formats
- ✅ Powerful data cleaning functions
- ✅ High performance with parallel processing support
- ✅ Both library usage and CLI support
- ✅ REST API / gRPC support (can be used as a microservice)

## Usage Examples

### As a Go Library

```go
import "github.com/mstgnz/cleango/pkg/cleaner"

func main() {
    // Serial processing
    df := cleaner.ReadCSV("data.csv")
    df.CleanDates("created_at", "2006-01-02")
    df.TrimColumns()
    df.WriteCSV("cleaned_data.csv")

    // Parallel processing
    df = cleaner.ReadCSV("big_data.csv")
    df.TrimColumnsParallel()
    df.CleanDatesParallel("created_at", "2006-01-02", cleaner.WithMaxWorkers(8))
    df.WriteCSV("cleaned_big_data.csv")
}
```

### As CLI

```bash
# Serial processing
cleango clean data.csv --trim --date-format="created_at:2006-01-02" --output=cleaned.csv

# Parallel processing
cleango clean big_data.csv --trim --date-format="created_at:2006-01-02" --parallel --workers=8 --output=cleaned.csv
```

### As a Microservice

```bash
docker run -p 8080:8080 cleango:latest
```

Request with REST API:

```
POST /clean
{
    "data": [...],
    "actions": ["trim", "normalize_dates:created_at=2006-01-02"],
    "parallel": true,
    "max_workers": 8
}
```

## Supported Formats

- CSV
- JSON
- Excel (xlsx, xls)
- Parquet

## Supported Cleaning Operations

| Operation          | Description                                   | Parallel Support |
| ------------------ | --------------------------------------------- | ---------------- |
| Trim               | Cleans the beginning/end of all cells         | ✅               |
| Null Replace       | Empty values are filled with defaults         | ✅               |
| Date Normalize     | Dates are converted to specified format       | ✅               |
| Case Normalize     | All strings are converted to lower/upper case | ✅               |
| Outlier Filter     | Values outside a specific range are removed   | ✅               |
| Column Rename      | Column names are normalized                   | ❌               |
| Custom Regex Clean | Cell cleaning with regex                      | ✅               |
| JSON Parse         | Converts string cell to JSON                  | ❌               |
| Column Split       | Single column is split into multiple columns  | ❌               |

## Installation

```bash
go get github.com/mstgnz/cleango
```

To install the CLI tool:

```bash
go install github.com/mstgnz/cleango/cmd/cleango@latest
```

## License

MIT
