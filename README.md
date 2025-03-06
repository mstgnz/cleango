# CleanGo - Data Cleaning at Go Speed 🚀

CleanGo is a library that performs data cleaning and transformation operations with the speed and efficiency of the Go language.

## Project Purpose

CleanGo aims to simplify and accelerate data cleaning processes, which are often the most time-consuming part of data analysis and machine learning workflows. By leveraging Go's performance and concurrency capabilities, CleanGo provides a robust toolkit for:

- **Efficient Data Processing**: Handle large datasets with minimal memory footprint and maximum CPU utilization
- **Format Flexibility**: Seamlessly work with various data formats (CSV, JSON, XML, YAML, Excel, Parquet) without needing multiple tools
- **Consistent API**: Use the same clean interface whether working with small or big data
- **Automation-Ready**: Integrate into data pipelines via library, CLI, or microservice approaches
- **Parallel Processing**: Utilize all available CPU cores for data cleaning tasks that can be parallelized

Whether you're a data scientist cleaning datasets for analysis, a developer building ETL pipelines, or a data engineer maintaining data quality, CleanGo provides the tools to make your data cleaning processes faster and more efficient.

## Use Cases

CleanGo is designed to address a variety of data cleaning challenges:

- **Data Preparation for Analysis**: Clean and transform raw data before feeding it into analysis tools or machine learning models
- **ETL Processes**: Extract data from various sources, transform it with cleaning operations, and load it into target systems
- **Data Migration**: Convert data between different formats while applying cleaning rules
- **Data Quality Assurance**: Standardize dates, normalize text, handle missing values, and remove outliers
- **Batch Processing**: Process large datasets efficiently with parallel execution
- **Microservice Architecture**: Deploy as a standalone service for data cleaning operations in distributed systems

## Target Audience

- **Data Scientists**: Who need to clean and prepare datasets for analysis and modeling
- **Data Engineers**: Building robust data pipelines that require cleaning steps
- **Backend Developers**: Integrating data cleaning capabilities into Go applications
- **DevOps Engineers**: Looking for efficient tools to include in data processing workflows
- **Analysts**: Who work with data from multiple sources and need to standardize formats

## Performance

CleanGo is built with performance in mind:

- **Parallel Processing**: Automatically distributes work across available CPU cores
- **Memory Efficiency**: Processes data in chunks to minimize memory usage
- **Optimized Algorithms**: Uses efficient algorithms for common cleaning operations
- **Go's Speed**: Leverages the performance benefits of compiled Go code
- **Benchmarked Operations**: Core operations are benchmarked to ensure optimal performance

In benchmark tests, CleanGo can process millions of rows per second on modern hardware, making it suitable for both small datasets and large-scale data processing tasks.

## Architecture

CleanGo follows a modular architecture:

- **Core DataFrame**: Central data structure that holds and manipulates tabular data
- **Format Handlers**: Specialized modules for reading/writing different file formats (CSV, JSON, XML, Excel, Parquet)
- **Cleaning Operations**: Individual functions for specific cleaning tasks
- **Parallel Framework**: Infrastructure for executing operations in parallel
- **CLI Layer**: Command-line interface for direct usage
- **API Layer**: REST and gRPC interfaces for service-oriented usage

This modular design allows for easy extension with new formats or cleaning operations while maintaining a consistent interface.

## Future Plans

The CleanGo project is actively developed with the following features planned for future releases:

- **Additional Data Formats**: Support for more specialized formats like Avro, ORC, and database connections
- **Advanced Cleaning Operations**: More sophisticated data cleaning algorithms including fuzzy matching and ML-based anomaly detection
- **Streaming Support**: Process data in streaming mode for real-time applications
- **Web UI**: A simple web interface for interactive data cleaning
- **Plugin System**: Allow third-party extensions for custom formats and operations
- **Cloud Integration**: Native connectors for cloud storage services (S3, GCS, Azure Blob)
- **Performance Optimizations**: Continuous improvements to processing speed and memory usage

## Features

- ✅ Reading and writing data in CSV, JSON, XML, YAML, Excel, and Parquet formats
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
- XML
- YAML
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
