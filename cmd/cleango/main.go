package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mstgnz/cleango/pkg/cleaner"
	"github.com/mstgnz/cleango/pkg/formats"
	"github.com/xitongsys/parquet-go/parquet"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: cleango <command> [arguments]")
		fmt.Println("Commands:")
		fmt.Println("  clean    Performs data cleaning operation")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "clean":
		if err := runClean(os.Args[2:]); err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown command %q.\n", os.Args[1])
		os.Exit(1)
	}
}

// runClean parses flags and args, then executes the clean command.
// Extracted from main() so it can be tested without os.Exit.
func runClean(args []string) error {
	cleanCmd := flag.NewFlagSet("clean", flag.ContinueOnError)

	trimFlag := cleanCmd.Bool("trim", false, "Clean whitespace at the beginning and end of all cells")
	dateFormatFlag := cleanCmd.String("date-format", "", "Date format (e.g.: created_at:2006-01-02)")
	nullReplaceFlag := cleanCmd.String("null-replace", "", "Replace empty values (e.g.: age:0,name:Unknown)")
	caseFlag := cleanCmd.String("case", "", "Upper/lower case conversion (e.g.: name:upper,description:lower)")
	outputFlag := cleanCmd.String("output", "", "Output file (default: cleaned_[input])")
	delimiterFlag := cleanCmd.String("delimiter", ",", "CSV delimiter character")
	formatFlag := cleanCmd.String("format", "", "Output format (csv, json, excel, parquet)")
	regexFlag := cleanCmd.String("regex", "", "Cleaning with regex (e.g.: name:[0-9]+:,description:\\s+: )")
	splitFlag := cleanCmd.String("split", "", "Column splitting (e.g.: full_name: :first_name,last_name)")
	outlierFlag := cleanCmd.String("outlier", "", "Outlier value filtering (e.g.: age:18:65)")
	sheetNameFlag := cleanCmd.String("sheet-name", "Sheet1", "Excel worksheet name")
	compressionFlag := cleanCmd.String("compression", "snappy", "Parquet compression algorithm (snappy, gzip, lz4, zstd, uncompressed)")
	parallelFlag := cleanCmd.Bool("parallel", false, "Use parallel processing")
	workersFlag := cleanCmd.Int("workers", 0, "Number of workers for parallel processing (0: as many as CPU cores)")

	if err := cleanCmd.Parse(args); err != nil {
		return err
	}

	positional := cleanCmd.Args()
	if len(positional) < 1 {
		return errors.New("input file not specified — usage: cleango clean [flags] <file>")
	}
	inputFile := positional[0]

	inputFormat := getFileFormat(inputFile)
	if inputFormat == "" {
		return errors.New("unsupported file format — supported: .csv, .json, .xlsx, .parquet")
	}

	outputFile := *outputFlag
	outputFormat := *formatFlag

	if outputFile == "" {
		outputFile = "cleaned_" + inputFile
	}
	if outputFormat == "" {
		outputFormat = inputFormat
	}

	var csvOptions []formats.CSVOption
	if *delimiterFlag != "" && len(*delimiterFlag) == 1 {
		csvOptions = append(csvOptions, formats.WithDelimiter(rune((*delimiterFlag)[0])))
	}

	var excelOptions []formats.ExcelOption
	if *sheetNameFlag != "" {
		excelOptions = append(excelOptions, formats.WithSheetName(*sheetNameFlag))
	}

	var parquetOptions []formats.ParquetOption
	switch strings.ToLower(*compressionFlag) {
	case "snappy":
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_SNAPPY))
	case "gzip":
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_GZIP))
	case "lz4":
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_LZ4))
	case "zstd":
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_ZSTD))
	case "uncompressed":
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_UNCOMPRESSED))
	default:
		parquetOptions = append(parquetOptions, formats.WithCompression(parquet.CompressionCodec_SNAPPY))
	}

	var parallelOptions []func(*cleaner.ParallelOptions)
	if *workersFlag > 0 {
		parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(*workersFlag))
	}

	var df *cleaner.DataFrame
	var err error

	switch inputFormat {
	case "csv":
		df, err = cleaner.ReadCSV(inputFile, csvOptions...)
	case "json":
		df, err = cleaner.ReadJSON(inputFile)
	case "excel":
		df, err = cleaner.ReadExcel(inputFile, excelOptions...)
	case "parquet":
		df, err = cleaner.ReadParquet(inputFile, parquetOptions...)
	}
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	if *parallelFlag {
		if err := applyParallel(df, trimFlag, dateFormatFlag, nullReplaceFlag, caseFlag, regexFlag, outlierFlag, parallelOptions); err != nil {
			return err
		}
	} else {
		if err := applySerial(df, trimFlag, dateFormatFlag, nullReplaceFlag, caseFlag, regexFlag, splitFlag, outlierFlag); err != nil {
			return err
		}
	}

	switch outputFormat {
	case "csv":
		err = df.WriteCSV(outputFile, csvOptions...)
	case "json":
		err = df.WriteJSON(outputFile)
	case "excel":
		err = df.WriteExcel(outputFile, excelOptions...)
	case "parquet":
		err = df.WriteParquet(outputFile, parquetOptions...)
	}
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	fmt.Printf("Cleaned data written to %s\n", outputFile)
	rowCount, colCount := df.Shape()
	fmt.Printf("Statistics: %d rows, %d columns\n", rowCount, colCount)
	return nil
}

func applyParallel(df *cleaner.DataFrame, trimFlag *bool, dateFormatFlag, nullReplaceFlag, caseFlag, regexFlag, outlierFlag *string, opts []func(*cleaner.ParallelOptions)) error {
	if *trimFlag {
		trimmed, err := df.TrimColumnsParallel(opts...)
		if err != nil {
			fmt.Printf("Trim error: %v\n", err)
		} else {
			*df = *trimmed
			fmt.Println("Trim operation applied in parallel")
		}
	}

	if *dateFormatFlag != "" {
		parts := strings.SplitN(*dateFormatFlag, ":", 2)
		if len(parts) == 2 {
			column, layout := parts[0], parts[1]
			if _, err := df.CleanDatesParallel(column, layout, opts...); err != nil {
				fmt.Printf("Date cleaning error: %v\n", err)
			} else {
				fmt.Printf("Date format cleaning applied in parallel for column %s\n", column)
			}
		}
	}

	if *nullReplaceFlag != "" {
		for _, replacement := range strings.Split(*nullReplaceFlag, ",") {
			parts := strings.SplitN(replacement, ":", 2)
			if len(parts) == 2 {
				column, value := parts[0], parts[1]
				if _, err := df.ReplaceNullsParallel(column, value, opts...); err != nil {
					fmt.Printf("Null replacement error: %v\n", err)
				} else {
					fmt.Printf("Null values in column %s replaced with %s in parallel\n", column, value)
				}
			}
		}
	}

	if *caseFlag != "" {
		for _, c := range strings.Split(*caseFlag, ",") {
			parts := strings.SplitN(c, ":", 2)
			if len(parts) == 2 {
				column, caseType := parts[0], parts[1]
				toUpper := strings.ToLower(caseType) == "upper"
				if _, err := df.NormalizeCaseParallel(column, toUpper, opts...); err != nil {
					fmt.Printf("Case conversion error: %v\n", err)
				} else {
					caseStr := "lower"
					if toUpper {
						caseStr = "upper"
					}
					fmt.Printf("%s case conversion applied in parallel for column %s\n", caseStr, column)
				}
			}
		}
	}

	if *regexFlag != "" {
		for _, r := range strings.Split(*regexFlag, ",") {
			parts := strings.SplitN(r, ":", 3)
			if len(parts) == 3 {
				column, pattern, replacement := parts[0], parts[1], parts[2]
				if _, err := df.CleanWithRegexParallel(column, pattern, replacement, opts...); err != nil {
					fmt.Printf("Regex cleaning error: %v\n", err)
				} else {
					fmt.Printf("Regex cleaning applied in parallel for column %s\n", column)
				}
			}
		}
	}

	if *outlierFlag != "" {
		for _, o := range strings.Split(*outlierFlag, ",") {
			parts := strings.SplitN(o, ":", 3)
			if len(parts) == 3 {
				column := parts[0]
				min, err1 := strconv.ParseFloat(parts[1], 64)
				max, err2 := strconv.ParseFloat(parts[2], 64)
				if err1 != nil || err2 != nil {
					fmt.Println("Outlier filtering error: invalid number")
					continue
				}
				if _, err := df.FilterOutliersParallel(column, min, max, opts...); err != nil {
					fmt.Printf("Outlier filtering error: %v\n", err)
				} else {
					fmt.Printf("Outliers filtered in column %s (min: %g, max: %g) in parallel\n", column, min, max)
				}
			}
		}
	}

	return nil
}

func applySerial(df *cleaner.DataFrame, trimFlag *bool, dateFormatFlag, nullReplaceFlag, caseFlag, regexFlag, splitFlag, outlierFlag *string) error {
	if *trimFlag {
		df.TrimColumns()
		fmt.Println("Trim operation applied")
	}

	if *dateFormatFlag != "" {
		parts := strings.SplitN(*dateFormatFlag, ":", 2)
		if len(parts) == 2 {
			column, layout := parts[0], parts[1]
			if _, err := df.CleanDates(column, layout); err != nil {
				fmt.Printf("Date cleaning error: %v\n", err)
			} else {
				fmt.Printf("Date format cleaning applied for column %s\n", column)
			}
		}
	}

	if *nullReplaceFlag != "" {
		for _, replacement := range strings.Split(*nullReplaceFlag, ",") {
			parts := strings.SplitN(replacement, ":", 2)
			if len(parts) == 2 {
				column, value := parts[0], parts[1]
				if _, err := df.ReplaceNulls(column, value); err != nil {
					fmt.Printf("Null replacement error: %v\n", err)
				} else {
					fmt.Printf("Null values in column %s replaced with %s\n", column, value)
				}
			}
		}
	}

	if *caseFlag != "" {
		for _, c := range strings.Split(*caseFlag, ",") {
			parts := strings.SplitN(c, ":", 2)
			if len(parts) == 2 {
				column, caseType := parts[0], parts[1]
				toUpper := strings.ToLower(caseType) == "upper"
				if _, err := df.NormalizeCase(column, toUpper); err != nil {
					fmt.Printf("Case conversion error: %v\n", err)
				} else {
					caseStr := "lower"
					if toUpper {
						caseStr = "upper"
					}
					fmt.Printf("%s case conversion applied for column %s\n", caseStr, column)
				}
			}
		}
	}

	if *regexFlag != "" {
		for _, r := range strings.Split(*regexFlag, ",") {
			parts := strings.SplitN(r, ":", 3)
			if len(parts) == 3 {
				column, pattern, replacement := parts[0], parts[1], parts[2]
				if _, err := df.CleanWithRegex(column, pattern, replacement); err != nil {
					fmt.Printf("Regex cleaning error: %v\n", err)
				} else {
					fmt.Printf("Regex cleaning applied for column %s\n", column)
				}
			}
		}
	}

	if *splitFlag != "" {
		for _, s := range strings.Split(*splitFlag, ",") {
			parts := strings.SplitN(s, ":", 3)
			if len(parts) >= 3 {
				column, separator := parts[0], parts[1]
				newColumns := strings.Split(parts[2], ",")
				if _, err := df.SplitColumn(column, separator, newColumns); err != nil {
					fmt.Printf("Column splitting error: %v\n", err)
				} else {
					fmt.Printf("Column %s split with %s\n", column, strings.Join(newColumns, ", "))
				}
			}
		}
	}

	if *outlierFlag != "" {
		for _, o := range strings.Split(*outlierFlag, ",") {
			parts := strings.SplitN(o, ":", 3)
			if len(parts) == 3 {
				column := parts[0]
				min, err1 := strconv.ParseFloat(parts[1], 64)
				max, err2 := strconv.ParseFloat(parts[2], 64)
				if err1 != nil || err2 != nil {
					fmt.Println("Outlier filtering error: invalid number")
					continue
				}
				if _, err := df.FilterOutliers(column, min, max); err != nil {
					fmt.Printf("Outlier filtering error: %v\n", err)
				} else {
					fmt.Printf("Outliers filtered in column %s (min: %g, max: %g)\n", column, min, max)
				}
			}
		}
	}

	return nil
}

func getFileFormat(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".csv":
		return "csv"
	case ".json":
		return "json"
	case ".xlsx", ".xls":
		return "excel"
	case ".parquet":
		return "parquet"
	default:
		return ""
	}
}
