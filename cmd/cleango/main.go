package main

import (
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
	// Define subcommands
	cleanCmd := flag.NewFlagSet("clean", flag.ExitOnError)

	// flags for clean command
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

	// Check arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: cleango <command> [arguments]")
		fmt.Println("Commands:")
		fmt.Println("  clean    Performs data cleaning operation")
		os.Exit(1)
	}

	// Determine subcommand
	switch os.Args[1] {
	case "clean":
		cleanCmd.Parse(os.Args[2:])
	default:
		fmt.Printf("Unknown command %q.\n", os.Args[1])
		os.Exit(1)
	}

	// process clean command
	if cleanCmd.Parsed() {
		// Check input file
		args := cleanCmd.Args()
		if len(args) < 1 {
			fmt.Println("Error: Input file not specified")
			fmt.Println("Usage: cleango clean [flags] <file>")
			os.Exit(1)
		}
		inputFile := args[0]

		// Determine input file format
		inputFormat := getFileFormat(inputFile)
		if inputFormat == "" {
			fmt.Println("Error: Unsupported file format")
			fmt.Println("Supported formats: .csv, .json, .xlsx, .parquet")
			os.Exit(1)
		}

		// Determine output file and format
		outputFile := *outputFlag
		outputFormat := *formatFlag

		if outputFile == "" {
			outputFile = "cleaned_" + inputFile
		}

		// If output format is not specified, use input format
		if outputFormat == "" {
			outputFormat = inputFormat
		}

		// Create CSV options
		var csvOptions []formats.CSVOption
		if *delimiterFlag != "" && len(*delimiterFlag) == 1 {
			csvOptions = append(csvOptions, formats.WithDelimiter(rune((*delimiterFlag)[0])))
		}

		// Create Excel options
		var excelOptions []formats.ExcelOption
		if *sheetNameFlag != "" {
			excelOptions = append(excelOptions, formats.WithSheetName(*sheetNameFlag))
		}

		// Create Parquet options
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

		// Create parallel processing options
		var parallelOptions []func(*cleaner.ParallelOptions)
		if *workersFlag > 0 {
			parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(*workersFlag))
		}

		// Read data
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
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}

		// Apply cleaning operations
		if *parallelFlag {
			// Parallel processing
			if *trimFlag {
				df = df.TrimColumnsParallel(parallelOptions...)
				fmt.Println("✓ Trim operation applied in parallel")
			}

			// Date format cleaning
			if *dateFormatFlag != "" {
				parts := strings.Split(*dateFormatFlag, ":")
				if len(parts) == 2 {
					column, layout := parts[0], parts[1]
					_, err := df.CleanDatesParallel(column, layout, parallelOptions...)
					if err != nil {
						fmt.Printf("Date cleaning error: %v\n", err)
					} else {
						fmt.Printf("✓ Date format cleaning applied in parallel for column %s\n", column)
					}
				}
			}

			// Replace null values
			if *nullReplaceFlag != "" {
				replacements := strings.Split(*nullReplaceFlag, ",")
				for _, replacement := range replacements {
					parts := strings.Split(replacement, ":")
					if len(parts) == 2 {
						column, value := parts[0], parts[1]
						_, err := df.ReplaceNullsParallel(column, value, parallelOptions...)
						if err != nil {
							fmt.Printf("Null replacement error: %v\n", err)
						} else {
							fmt.Printf("✓ Null values in column %s replaced with %s in parallel\n", column, value)
						}
					}
				}
			}

			// Upper/lower case conversion
			if *caseFlag != "" {
				cases := strings.Split(*caseFlag, ",")
				for _, c := range cases {
					parts := strings.Split(c, ":")
					if len(parts) == 2 {
						column, caseType := parts[0], parts[1]
						toUpper := strings.ToLower(caseType) == "upper"
						_, err := df.NormalizeCaseParallel(column, toUpper, parallelOptions...)
						if err != nil {
							fmt.Printf("Case conversion error: %v\n", err)
						} else {
							caseStr := "lower"
							if toUpper {
								caseStr = "upper"
							}
							fmt.Printf("✓ %s case conversion applied in parallel for column %s\n", caseStr, column)
						}
					}
				}
			}

			// Regex cleaning
			if *regexFlag != "" {
				regexes := strings.Split(*regexFlag, ",")
				for _, r := range regexes {
					parts := strings.Split(r, ":")
					if len(parts) == 3 {
						column, pattern, replacement := parts[0], parts[1], parts[2]
						_, err := df.CleanWithRegexParallel(column, pattern, replacement, parallelOptions...)
						if err != nil {
							fmt.Printf("Regex cleaning error: %v\n", err)
						} else {
							fmt.Printf("✓ Regex cleaning applied in parallel for column %s\n", column)
						}
					}
				}
			}

			// Outlier filtering
			if *outlierFlag != "" {
				outliers := strings.Split(*outlierFlag, ",")
				for _, o := range outliers {
					parts := strings.Split(o, ":")
					if len(parts) == 3 {
						column := parts[0]
						min, err1 := strconv.ParseFloat(parts[1], 64)
						max, err2 := strconv.ParseFloat(parts[2], 64)
						if err1 != nil || err2 != nil {
							fmt.Printf("Outlier filtering error: Invalid number\n")
							continue
						}
						_, err := df.FilterOutliersParallel(column, min, max, parallelOptions...)
						if err != nil {
							fmt.Printf("Outlier filtering error: %v\n", err)
						} else {
							fmt.Printf("✓ Outliers filtered in column %s (min: %g, max: %g) in parallel\n", column, min, max)
						}
					}
				}
			}
		} else {
			// Serial processing
			if *trimFlag {
				df.TrimColumns()
				fmt.Println("✓ Trim operation applied")
			}

			// Date format cleaning
			if *dateFormatFlag != "" {
				parts := strings.Split(*dateFormatFlag, ":")
				if len(parts) == 2 {
					column, layout := parts[0], parts[1]
					_, err := df.CleanDates(column, layout)
					if err != nil {
						fmt.Printf("Date cleaning error: %v\n", err)
					} else {
						fmt.Printf("✓ Date format cleaning applied for column %s\n", column)
					}
				}
			}

			// Replace null values
			if *nullReplaceFlag != "" {
				replacements := strings.Split(*nullReplaceFlag, ",")
				for _, replacement := range replacements {
					parts := strings.Split(replacement, ":")
					if len(parts) == 2 {
						column, value := parts[0], parts[1]
						_, err := df.ReplaceNulls(column, value)
						if err != nil {
							fmt.Printf("Null replacement error: %v\n", err)
						} else {
							fmt.Printf("✓ Null values in column %s replaced with %s\n", column, value)
						}
					}
				}
			}

			// Upper/lower case conversion
			if *caseFlag != "" {
				cases := strings.Split(*caseFlag, ",")
				for _, c := range cases {
					parts := strings.Split(c, ":")
					if len(parts) == 2 {
						column, caseType := parts[0], parts[1]
						toUpper := strings.ToLower(caseType) == "upper"
						_, err := df.NormalizeCase(column, toUpper)
						if err != nil {
							fmt.Printf("Case conversion error: %v\n", err)
						} else {
							caseStr := "lower"
							if toUpper {
								caseStr = "upper"
							}
							fmt.Printf("✓ %s case conversion applied for column %s\n", caseStr, column)
						}
					}
				}
			}

			// Regex cleaning
			if *regexFlag != "" {
				regexes := strings.Split(*regexFlag, ",")
				for _, r := range regexes {
					parts := strings.Split(r, ":")
					if len(parts) == 3 {
						column, pattern, replacement := parts[0], parts[1], parts[2]
						_, err := df.CleanWithRegex(column, pattern, replacement)
						if err != nil {
							fmt.Printf("Regex cleaning error: %v\n", err)
						} else {
							fmt.Printf("✓ Regex cleaning applied for column %s\n", column)
						}
					}
				}
			}

			// Column splitting
			if *splitFlag != "" {
				splits := strings.Split(*splitFlag, ",")
				for _, s := range splits {
					parts := strings.Split(s, ":")
					if len(parts) >= 3 {
						column, separator := parts[0], parts[1]
						newColumns := strings.Split(parts[2], ",")
						_, err := df.SplitColumn(column, separator, newColumns)
						if err != nil {
							fmt.Printf("Column splitting error: %v\n", err)
						} else {
							fmt.Printf("✓ Column %s split with %s\n", column, strings.Join(newColumns, ", "))
						}
					}
				}
			}

			// Outlier filtering
			if *outlierFlag != "" {
				outliers := strings.Split(*outlierFlag, ",")
				for _, o := range outliers {
					parts := strings.Split(o, ":")
					if len(parts) == 3 {
						column := parts[0]
						min, err1 := strconv.ParseFloat(parts[1], 64)
						max, err2 := strconv.ParseFloat(parts[2], 64)
						if err1 != nil || err2 != nil {
							fmt.Printf("Outlier filtering error: Invalid number\n")
							continue
						}
						_, err := df.FilterOutliers(column, min, max)
						if err != nil {
							fmt.Printf("Outlier filtering error: %v\n", err)
						} else {
							fmt.Printf("✓ Outliers filtered in column %s (min: %g, max: %g)\n", column, min, max)
						}
					}
				}
			}
		}

		// Write result
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
			fmt.Printf("Output writing error: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✓ Cleaned data written to %s\n", outputFile)

		// Statistics
		rowCount, colCount := df.Shape()
		fmt.Printf("📊 Statistics: %d rows, %d columns\n", rowCount, colCount)
	}
}

// getFileFormat, returns format based on file extension
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
