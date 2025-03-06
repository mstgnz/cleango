package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/mstgnz/cleango/pkg/cleaner"
	"github.com/mstgnz/cleango/pkg/formats"
	"github.com/xitongsys/parquet-go/parquet"
)

func main() {
	// Ensure examples directory exists
	examplesDir := "examples"
	if _, err := os.Stat(examplesDir); os.IsNotExist(err) {
		examplesDir = "."
	}

	// Example 1: Basic CSV Processing
	fmt.Println("=== Example 1: Basic CSV Processing ===")
	basicCSVExample(examplesDir)

	// Example 2: JSON Processing
	fmt.Println("\n=== Example 2: JSON Processing ===")
	jsonExample(examplesDir)

	// Example 3: XML Processing
	fmt.Println("\n=== Example 3: XML Processing ===")
	xmlExample(examplesDir)

	// Example 4: Excel Processing
	fmt.Println("\n=== Example 4: Excel Processing ===")
	excelExample()

	// Example 5: Parquet Processing
	fmt.Println("\n=== Example 5: Parquet Processing ===")
	parquetExample()

	// Example 6: Format Conversion
	fmt.Println("\n=== Example 6: Format Conversion ===")
	formatConversionExample(examplesDir)

	// Example 7: Parallel Processing
	fmt.Println("\n=== Example 7: Parallel Processing ===")
	parallelProcessingExample(examplesDir)

	// Example 8: Advanced Cleaning Operations
	fmt.Println("\n=== Example 8: Advanced Cleaning Operations ===")
	advancedCleaningExample()

	// Example 9: Custom Data Creation and Manipulation
	fmt.Println("\n=== Example 9: Custom Data Creation and Manipulation ===")
	customDataExample()

	// Example 10: Error Handling
	fmt.Println("\n=== Example 10: Error Handling ===")
	errorHandlingExample()

	fmt.Println("\nAll examples completed successfully!")
}

// Example 1: Basic CSV Processing
func basicCSVExample(examplesDir string) {
	// Read CSV file
	csvPath := filepath.Join(examplesDir, "sample_data.csv")
	df, err := cleaner.ReadCSV(csvPath)
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	// Display initial data
	rows, cols := df.Shape()
	fmt.Printf("CSV loaded: %d rows, %d columns\n", rows, cols)
	fmt.Printf("Headers: %v\n", df.GetHeaders())

	// Basic cleaning operations
	df.TrimColumns()
	fmt.Println("Applied trim to all columns")

	// Clean dates
	df, err = df.CleanDates("created_at", "2006-01-02")
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Normalized dates in 'created_at' column")
	}

	// Replace nulls
	df, err = df.ReplaceNulls("age", "0")
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Replaced null values in 'age' column with '0'")
	}

	// Save to a new CSV file
	outputPath := "cleaned_csv_example.csv"
	err = df.WriteCSV(outputPath)
	if err != nil {
		log.Printf("Error writing CSV: %v", err)
	} else {
		fmt.Printf("Saved cleaned data to %s\n", outputPath)
	}
}

// Example 2: JSON Processing
func jsonExample(examplesDir string) {
	// Read JSON file
	jsonPath := filepath.Join(examplesDir, "sample_data.json")
	df, err := cleaner.ReadJSON(jsonPath)
	if err != nil {
		log.Fatalf("Error reading JSON: %v", err)
	}

	// Display initial data
	rows, cols := df.Shape()
	fmt.Printf("JSON loaded: %d rows, %d columns\n", rows, cols)

	// Normalize case for specific columns
	df, err = df.NormalizeCase("name", false) // lowercase
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Converted 'name' column to lowercase")
	}

	// Save to a new JSON file with pretty formatting
	outputPath := "cleaned_json_example.json"
	err = df.WriteJSON(outputPath, formats.WithPretty(true))
	if err != nil {
		log.Printf("Error writing JSON: %v", err)
	} else {
		fmt.Printf("Saved cleaned data to %s\n", outputPath)
	}
}

// Example 3: XML Processing
func xmlExample(examplesDir string) {
	// Read XML file
	xmlPath := filepath.Join(examplesDir, "sample_data.xml")
	df, err := cleaner.ReadXML(xmlPath,
		formats.WithXMLRootElement("root"),
		formats.WithXMLItemElement("item"))
	if err != nil {
		log.Fatalf("Error reading XML: %v", err)
	}

	// Display initial data
	rows, cols := df.Shape()
	fmt.Printf("XML loaded: %d rows, %d columns\n", rows, cols)

	// Clean with regex
	df, err = df.CleanWithRegex("email", "@example\\.com", "@cleango.org")
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Replaced email domains from @example.com to @cleango.org")
	}

	// Save to a new XML file with custom root and item elements
	outputPath := "cleaned_xml_example.xml"
	err = df.WriteXML(outputPath,
		formats.WithXMLPretty(true),
		formats.WithXMLRootElement("users"),
		formats.WithXMLItemElement("user"))
	if err != nil {
		log.Printf("Error writing XML: %v", err)
	} else {
		fmt.Printf("Saved cleaned data to %s\n", outputPath)
	}
}

// Example 4: Excel Processing
func excelExample() {
	// Since we might not have an Excel file in the examples, we'll create a DataFrame and save it as Excel
	// Create a simple DataFrame
	headers := []string{"id", "name", "email", "age", "created_at"}
	data := [][]string{
		{"1", "John Doe", "john@example.com", "30", "2023-01-15"},
		{"2", "Jane Smith", "jane@example.com", "25", "2023-02-20"},
		{"3", "Bob Johnson", "bob@example.com", "40", "2023-03-10"},
	}

	df, err := cleaner.NewDataFrame(headers, data)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Display initial data
	rows, cols := df.Shape()
	fmt.Printf("Excel example DataFrame created: %d rows, %d columns\n", rows, cols)

	// Save to Excel file
	outputPath := "excel_example.xlsx"
	err = df.WriteExcel(outputPath, formats.WithSheetName("CleanGo Example"))
	if err != nil {
		log.Printf("Error writing Excel: %v", err)
	} else {
		fmt.Printf("Saved data to %s\n", outputPath)
	}

	// Read back the Excel file if it exists
	if _, err := os.Stat(outputPath); err == nil {
		dfRead, err := cleaner.ReadExcel(outputPath)
		if err != nil {
			log.Printf("Error reading Excel: %v", err)
		} else {
			rowsRead, colsRead := dfRead.Shape()
			fmt.Printf("Excel file read back: %d rows, %d columns\n", rowsRead, colsRead)
		}
	}
}

// Example 5: Parquet Processing
func parquetExample() {
	// Create a DataFrame for Parquet example
	headers := []string{"id", "name", "email", "age", "created_at"}
	data := [][]string{
		{"1", "John Doe", "john@example.com", "30", "2023-01-15"},
		{"2", "Jane Smith", "jane@example.com", "25", "2023-02-20"},
		{"3", "Bob Johnson", "bob@example.com", "40", "2023-03-10"},
	}

	df, err := cleaner.NewDataFrame(headers, data)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Display initial data
	rows, cols := df.Shape()
	fmt.Printf("Parquet example DataFrame created: %d rows, %d columns\n", rows, cols)

	// Save to Parquet file
	outputPath := "parquet_example.parquet"
	err = df.WriteParquet(outputPath, cleaner.WithParquetCompression(parquet.CompressionCodec_SNAPPY))
	if err != nil {
		log.Printf("Error writing Parquet: %v", err)
	} else {
		fmt.Printf("Saved data to %s\n", outputPath)
	}

	// Read back the Parquet file if it exists
	if _, err := os.Stat(outputPath); err == nil {
		dfRead, err := cleaner.ReadParquet(outputPath)
		if err != nil {
			log.Printf("Error reading Parquet: %v", err)
		} else {
			rowsRead, colsRead := dfRead.Shape()
			fmt.Printf("Parquet file read back: %d rows, %d columns\n", rowsRead, colsRead)
		}
	}
}

// Example 6: Format Conversion
func formatConversionExample(examplesDir string) {
	// Read CSV file
	csvPath := filepath.Join(examplesDir, "sample_data.csv")
	df, err := cleaner.ReadCSV(csvPath)
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	rows, cols := df.Shape()
	fmt.Printf("CSV loaded for format conversion: %d rows, %d columns\n", rows, cols)

	// Convert to JSON
	jsonOutputPath := "format_conversion_example.json"
	err = df.WriteJSON(jsonOutputPath, formats.WithPretty(true))
	if err != nil {
		log.Printf("Error converting to JSON: %v", err)
	} else {
		fmt.Printf("Converted CSV to JSON: %s\n", jsonOutputPath)
	}

	// Convert to XML
	xmlOutputPath := "format_conversion_example.xml"
	err = df.WriteXML(xmlOutputPath,
		formats.WithXMLPretty(true),
		formats.WithXMLRootElement("data"),
		formats.WithXMLItemElement("record"))
	if err != nil {
		log.Printf("Error converting to XML: %v", err)
	} else {
		fmt.Printf("Converted CSV to XML: %s\n", xmlOutputPath)
	}

	// Convert to Excel
	excelOutputPath := "format_conversion_example.xlsx"
	err = df.WriteExcel(excelOutputPath)
	if err != nil {
		log.Printf("Error converting to Excel: %v", err)
	} else {
		fmt.Printf("Converted CSV to Excel: %s\n", excelOutputPath)
	}

	// Convert to Parquet
	parquetOutputPath := "format_conversion_example.parquet"
	err = df.WriteParquet(parquetOutputPath)
	if err != nil {
		log.Printf("Error converting to Parquet: %v", err)
	} else {
		fmt.Printf("Converted CSV to Parquet: %s\n", parquetOutputPath)
	}
}

// Example 7: Parallel Processing
func parallelProcessingExample(examplesDir string) {
	// Read CSV file
	csvPath := filepath.Join(examplesDir, "sample_data.csv")
	df, err := cleaner.ReadCSV(csvPath)
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	rows, cols := df.Shape()
	fmt.Printf("CSV loaded for parallel processing: %d rows, %d columns\n", rows, cols)

	// Set worker count
	workers := cleaner.WithMaxWorkers(4)
	fmt.Println("Using 4 worker threads for parallel processing")

	// Start timing
	start := time.Now()

	// Parallel trim
	df = df.TrimColumnsParallel()
	fmt.Println("Applied parallel trim to all columns")

	// Parallel date cleaning
	df, err = df.CleanDatesParallel("created_at", "2006-01-02", workers)
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Normalized dates in 'created_at' column in parallel")
	}

	// Parallel null replacement
	df, err = df.ReplaceNullsParallel("age", "0", workers)
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Replaced null values in 'age' column with '0' in parallel")
	}

	// Parallel case normalization
	df, err = df.NormalizeCaseParallel("name", false, workers) // lowercase
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Converted 'name' column to lowercase in parallel")
	}

	// Batch processing with multiple operations
	processors := []func(*cleaner.DataFrame) (*cleaner.DataFrame, error){
		func(df *cleaner.DataFrame) (*cleaner.DataFrame, error) {
			return df.CleanWithRegexParallel("email", "@example\\.com", "@cleango.org", workers)
		},
		func(df *cleaner.DataFrame) (*cleaner.DataFrame, error) {
			return df.FilterOutliersParallel("age", 18, 65, workers)
		},
	}

	df, err = df.BatchProcessParallel(processors, workers)
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Applied batch processing with multiple operations in parallel")
	}

	// End timing
	elapsed := time.Since(start)
	fmt.Printf("Parallel processing completed in %s\n", elapsed)

	// Save result
	outputPath := "parallel_processing_example.csv"
	err = df.WriteCSV(outputPath)
	if err != nil {
		log.Printf("Error writing CSV: %v", err)
	} else {
		fmt.Printf("Saved parallel processed data to %s\n", outputPath)
	}
}

// Example 8: Advanced Cleaning Operations
func advancedCleaningExample() {
	// Create a DataFrame with some complex data
	headers := []string{"id", "full_name", "email", "phone", "age", "score", "json_data", "created_at"}
	data := [][]string{
		{"1", "John Doe", "john@example.com", "123-456-7890", "30", "85.5", `{"city":"New York","country":"USA"}`, "2023-01-15"},
		{"2", "Jane Smith", "jane@example.com", "(555) 123-4567", "25", "92.0", `{"city":"London","country":"UK"}`, "2023-02-20"},
		{"3", "Bob Johnson", "bob@example.com", "987.654.3210", "40", "78.3", `{"city":"Paris","country":"France"}`, "2023-03-10"},
		{"4", "Alice Brown", "alice@example.com", "555-555-5555", "35", "88.7", `{"city":"Berlin","country":"Germany"}`, "2023-04-05"},
		{"5", "Charlie Wilson", "charlie@example.com", "111.222.3333", "28", "76.2", `{"city":"Tokyo","country":"Japan"}`, "2023-05-12"},
	}

	df, err := cleaner.NewDataFrame(headers, data)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	rows, cols := df.Shape()
	fmt.Printf("Advanced cleaning example DataFrame created: %d rows, %d columns\n", rows, cols)

	// Split full_name into first_name and last_name
	df, err = df.SplitColumn("full_name", " ", []string{"first_name", "last_name"})
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Split 'full_name' into 'first_name' and 'last_name'")
	}

	// Normalize phone numbers with regex
	df, err = df.CleanWithRegex("phone", "[^0-9]", "")
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Normalized phone numbers by removing non-numeric characters")
	}

	// Filter outliers in score column
	df, err = df.FilterOutliers("score", 80.0, 95.0)
	if err != nil {
		log.Printf("Warning: %v", err)
	} else {
		fmt.Println("Filtered outliers in 'score' column (keeping values between 80.0 and 95.0)")
	}

	// Save result
	outputPath := "advanced_cleaning_example.csv"
	err = df.WriteCSV(outputPath)
	if err != nil {
		log.Printf("Error writing CSV: %v", err)
	} else {
		fmt.Printf("Saved advanced cleaned data to %s\n", outputPath)
	}
}

// Example 9: Custom Data Creation and Manipulation
func customDataExample() {
	// Create a DataFrame from scratch
	headers := []string{"id", "product", "category", "price", "in_stock", "last_updated"}
	data := [][]string{
		{"1", "Laptop", "Electronics", "1200.00", "true", "2023-01-10"},
		{"2", "Desk Chair", "Furniture", "150.50", "true", "2023-02-15"},
		{"3", "Coffee Maker", "Appliances", "89.99", "false", "2023-03-20"},
		{"4", "Headphones", "Electronics", "79.95", "true", "2023-04-25"},
		{"5", "Bookshelf", "Furniture", "199.99", "true", "2023-05-30"},
	}

	df, err := cleaner.NewDataFrame(headers, data)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	rows, cols := df.Shape()
	fmt.Printf("Custom DataFrame created: %d rows, %d columns\n", rows, cols)

	// Note: The following methods might not be implemented in the current version
	// Uncomment if they are available in your version of CleanGo

	/*
		// Add a new column with calculated values
		newColumn := make([]string, len(data))
		for i, row := range df.GetData() {
			// Find price column index
			priceIdx := -1
			for j, header := range df.GetHeaders() {
				if header == "price" {
					priceIdx = j
					break
				}
			}

			if priceIdx >= 0 {
				// Parse price and add tax
				price := 0.0
				fmt.Sscanf(row[priceIdx], "%f", &price)
				taxPrice := price * 1.08 // 8% tax
				newColumn[i] = fmt.Sprintf("%.2f", taxPrice)
			}
		}

		df, err = df.AddColumn("price_with_tax", newColumn)
		if err != nil {
			log.Printf("Warning: %v", err)
		} else {
			fmt.Println("Added 'price_with_tax' column with calculated values")
		}

		// Filter rows by category
		df, err = df.FilterRows("category", "Electronics")
		if err != nil {
			log.Printf("Warning: %v", err)
		} else {
			fmt.Println("Filtered rows to keep only 'Electronics' category")
		}

		// Sort by price
		df, err = df.SortByColumn("price", true) // descending order
		if err != nil {
			log.Printf("Warning: %v", err)
		} else {
			fmt.Println("Sorted rows by 'price' in descending order")
		}
	*/

	// Save result
	outputPath := "custom_data_example.csv"
	err = df.WriteCSV(outputPath)
	if err != nil {
		log.Printf("Error writing CSV: %v", err)
	} else {
		fmt.Printf("Saved custom data to %s\n", outputPath)
	}
}

// Example 10: Error Handling
func errorHandlingExample() {
	// Attempt to read a non-existent file
	_, err := cleaner.ReadCSV("non_existent_file.csv")
	if err != nil {
		fmt.Println("Expected error handling: Successfully caught error when reading non-existent file")
	}

	// Create a DataFrame with mismatched columns
	headers := []string{"id", "name", "email"}
	data := [][]string{
		{"1", "John Doe", "john@example.com"},
		{"2", "Jane Smith"}, // Missing email
	}

	_, err = cleaner.NewDataFrame(headers, data)
	if err != nil {
		fmt.Println("Expected error handling: Successfully caught error with mismatched column count")
	}

	// Create a valid DataFrame for further error tests
	validData := [][]string{
		{"1", "John Doe", "john@example.com"},
		{"2", "Jane Smith", "jane@example.com"},
	}

	df, err := cleaner.NewDataFrame(headers, validData)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Try to access a non-existent column
	_, err = df.CleanDates("non_existent_column", "2006-01-02")
	if err != nil {
		fmt.Println("Expected error handling: Successfully caught error when accessing non-existent column")
	}

	// Try to parse invalid date
	// Note: AddColumn might not be implemented in the current version
	// We'll use a different approach for demonstration

	// Create a DataFrame with invalid dates
	headersWithDate := []string{"id", "name", "invalid_date"}
	dataWithDate := [][]string{
		{"1", "John Doe", "not-a-date"},
		{"2", "Jane Smith", "also-not-a-date"},
	}

	dfWithDate, err := cleaner.NewDataFrame(headersWithDate, dataWithDate)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	_, err = dfWithDate.CleanDates("invalid_date", "2006-01-02")
	if err != nil {
		fmt.Println("Expected error handling: Successfully caught error when parsing invalid date")
	}

	fmt.Println("Error handling example completed successfully")
}
