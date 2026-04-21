# CleanGo - Data Cleaning at Go Speed

CleanGo is a library that performs data cleaning and transformation operations with the speed and efficiency of the Go language.

[![CI](https://github.com/mstgnz/cleango/actions/workflows/ci.yml/badge.svg)](https://github.com/mstgnz/cleango/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mstgnz/cleango.svg)](https://pkg.go.dev/github.com/mstgnz/cleango)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Project Purpose

CleanGo aims to simplify and accelerate data cleaning processes, which are often the most time-consuming part of data analysis and machine learning workflows. By leveraging Go's performance and concurrency capabilities, CleanGo provides a robust toolkit for:

- **Efficient Data Processing**: Handle large datasets with minimal memory footprint and maximum CPU utilization
- **Format Flexibility**: Seamlessly work with various data formats (CSV, JSON, XML, YAML, Excel, Parquet) without needing multiple tools
- **Consistent API**: Use the same clean interface whether working with small or big data
- **Automation-Ready**: Integrate into data pipelines via library, CLI, or REST microservice
- **Parallel Processing**: Utilize all available CPU cores for data cleaning tasks
- **Context-Aware**: Support cancellation and timeouts for long-running operations

## Features

- Reading and writing data in CSV, JSON, XML, YAML, Excel, and Parquet formats
- Powerful data cleaning functions
- High performance with parallel processing support
- Context support for cancellation and timeout control
- Both library usage and CLI support
- REST API support (deployable as a microservice)

## Installation

```bash
go get github.com/mstgnz/cleango
```

To install the CLI tool:

```bash
go install github.com/mstgnz/cleango/cmd/cleango@latest
```

## Usage Examples

### As a Go Library

```go
import "github.com/mstgnz/cleango/pkg/cleaner"

func main() {
    // Read data
    df, err := cleaner.ReadCSV("data.csv")
    if err != nil {
        log.Fatal(err)
    }

    // Serial processing
    df.TrimColumns()

    if _, err := df.CleanDates("created_at", "2006-01-02"); err != nil {
        log.Printf("date cleaning error: %v", err)
    }

    if err := df.WriteCSV("cleaned_data.csv"); err != nil {
        log.Fatal(err)
    }
}
```

#### Parallel Processing

```go
df, err := cleaner.ReadCSV("big_data.csv")
if err != nil {
    log.Fatal(err)
}

// Parallel trim with 8 workers
df, err = df.TrimColumnsParallel(cleaner.WithMaxWorkers(8))
if err != nil {
    log.Fatal(err)
}

// Parallel date cleaning
df, err = df.CleanDatesParallel("created_at", "2006-01-02", cleaner.WithMaxWorkers(8))
if err != nil {
    log.Fatal(err)
}
```

#### Context Support (Cancellation and Timeout)

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

df, err = df.TrimColumnsParallel(cleaner.WithContext(ctx), cleaner.WithMaxWorkers(4))
if err != nil {
    // err is context.DeadlineExceeded or context.Canceled
    log.Fatal(err)
}
```

### As CLI

```bash
# Serial processing
cleango clean data.csv --trim --date-format="created_at:2006-01-02" --output=cleaned.csv

# Parallel processing
cleango clean big_data.csv --trim --date-format="created_at:2006-01-02" --parallel --workers=8 --output=cleaned.csv

# Replace nulls and normalize case
cleango clean data.csv --null-replace="age:0,name:Unknown" --case="name:upper" --output=cleaned.csv

# Filter outliers
cleango clean data.csv --outlier="salary:1000:100000" --output=cleaned.csv

# Regex cleaning
cleango clean data.csv --regex="phone:[^0-9]:" --output=cleaned.csv
```

### As a REST Microservice

```bash
docker run -p 8080:8080 cleango:latest
```

#### Clean in-memory data

```
POST /clean
Content-Type: application/json

{
    "data": [
        {"name": "  Alice  ", "created_at": "2024-01-15", "salary": "5000"},
        {"name": "  Bob  ",   "created_at": "2024-02-20", "salary": "99999"}
    ],
    "actions": [
        "trim",
        "normalize_dates:created_at=2006-01-02",
        "normalize_case:name=upper",
        "filter_outliers:salary=1000=50000"
    ],
    "parallel": true,
    "max_workers": 4
}
```

#### Clean a file on the server

```
POST /clean-file
Content-Type: application/json

{
    "file_path": "data/input.csv",
    "actions": ["trim", "replace_nulls:age=0"],
    "format": "json",
    "output": "data/output.json"
}
```

#### Health check

```
GET /health
```

## Supported Formats

| Format  | Read | Write |
|---------|------|-------|
| CSV     | Yes  | Yes   |
| JSON    | Yes  | Yes   |
| XML     | Yes  | Yes   |
| YAML    | Yes  | Yes   |
| Excel   | Yes  | Yes   |
| Parquet | Yes  | Yes   |

## Supported Cleaning Operations

| Operation       | Description                                   | Parallel Support |
|-----------------|-----------------------------------------------|------------------|
| Trim            | Remove leading/trailing whitespace from cells | Yes              |
| Null Replace    | Fill empty values with a default              | Yes              |
| Date Normalize  | Convert dates to a specified format           | Yes              |
| Case Normalize  | Convert strings to upper or lower case        | Yes              |
| Outlier Filter  | Remove values outside a specified range       | Yes              |
| Regex Clean     | Clean cell values using a regex pattern       | Yes              |
| Column Split    | Split one column into multiple columns        | No               |
| Column Rename   | Rename a column                               | No               |

## API Actions Reference

Actions are passed as strings in the format `action_type:parameters`.

| Action            | Format                              | Example                                    |
|-------------------|-------------------------------------|--------------------------------------------|
| `trim`            | `trim`                              | `"trim"`                                   |
| `normalize_dates` | `normalize_dates:column=layout`     | `"normalize_dates:created_at=2006-01-02"`  |
| `replace_nulls`   | `replace_nulls:column=value`        | `"replace_nulls:age=0"`                    |
| `normalize_case`  | `normalize_case:column=upper\|lower` | `"normalize_case:name=upper"`             |
| `clean_regex`     | `clean_regex:column=pattern=replace`| `"clean_regex:phone=[^0-9]="`              |
| `split_column`    | `split_column:column=sep=col1,col2` | `"split_column:full_name= =first,last"`    |
| `filter_outliers` | `filter_outliers:column=min=max`    | `"filter_outliers:salary=1000=100000"`     |

## Architecture

CleanGo follows a modular architecture:

- **`pkg/cleaner`** — Core DataFrame type, all cleaning operations, and parallel processing framework
- **`pkg/formats`** — Format-specific read/write handlers (CSV, JSON, XML, YAML, Excel, Parquet)
- **`cmd/cleango`** — CLI application
- **`cmd/api`** — REST API server with graceful shutdown and request context propagation

## Use Cases

- **Data preparation for analysis**: Clean and transform raw data before feeding it into analysis tools or ML models
- **ETL processes**: Extract, transform, and load data across different formats
- **Data migration**: Convert data between formats while applying cleaning rules
- **Data quality assurance**: Standardize dates, normalize text, handle missing values, remove outliers
- **Microservice architecture**: Deploy as a standalone REST service in distributed systems

## Performance

CleanGo is built with performance in mind:

- **Parallel processing**: Distributes work across all available CPU cores using a configurable worker pool
- **Memory efficiency**: Processes data without unnecessary copies
- **Deterministic output**: Column ordering is consistent across all format readers
- **Race-condition free**: All parallel operations are verified with Go's race detector

## Future Plans

- Additional formats: Avro, ORC, direct database connections
- Advanced cleaning: fuzzy matching, ML-based anomaly detection
- Streaming mode for real-time applications
- Cloud storage connectors (S3, GCS, Azure Blob)

## License

MIT
