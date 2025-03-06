package cleaner

import (
	"github.com/mstgnz/cleango/pkg/formats"
	"github.com/xitongsys/parquet-go/parquet"
)

// ReadParquet, Parquet file is read and converted to DataFrame
func ReadParquet(filePath string, options ...formats.ParquetOption) (*DataFrame, error) {
	headers, data, err := formats.ReadParquetToRaw(filePath, options...)
	if err != nil {
		return nil, err
	}

	return NewDataFrame(headers, data)
}

// WriteParquet, DataFrame is written to Parquet file
func (df *DataFrame) WriteParquet(filePath string, options ...formats.ParquetOption) error {
	return formats.WriteParquet(df, filePath, options...)
}

// WithParquetCompression, Parquet compression algorithm is determined
func WithParquetCompression(compression parquet.CompressionCodec) formats.ParquetOption {
	return formats.WithCompression(compression)
}
