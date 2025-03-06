package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mstgnz/cleango/pkg/cleaner"
)

// CleanRequest, structure for cleanup request
type CleanRequest struct {
	Data       []map[string]interface{} `json:"data"`
	Actions    []string                 `json:"actions"`
	Format     string                   `json:"format,omitempty"`      // Output format (csv, json, excel, parquet)
	Parallel   bool                     `json:"parallel,omitempty"`    // Use parallel processing
	MaxWorkers int                      `json:"max_workers,omitempty"` // Number of workers for parallel processing (0: as many as CPU cores)
}

// CleanResponse, structure for cleanup response
type CleanResponse struct {
	Data       []map[string]interface{} `json:"data"`
	Statistics map[string]int           `json:"statistics"`
	Message    string                   `json:"message"`
}

// FileCleanRequest, structure for file cleanup request
type FileCleanRequest struct {
	FilePath   string   `json:"file_path"`
	Actions    []string `json:"actions"`
	Format     string   `json:"format,omitempty"`      // Output format (csv, json, excel, parquet)
	Output     string   `json:"output,omitempty"`      // Output file
	Parallel   bool     `json:"parallel,omitempty"`    // Use parallel processing
	MaxWorkers int      `json:"max_workers,omitempty"` // Number of workers for parallel processing (0: as many as CPU cores)
}

func main() {
	// Determine port number
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Create HTTP router
	http.HandleFunc("/clean", handleClean)
	http.HandleFunc("/clean-file", handleCleanFile)
	http.HandleFunc("/health", handleHealth)

	// Start server
	log.Printf("CleanGo API %s portunda başlatılıyor...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Sunucu başlatılamadı: %v", err)
	}
}

// handleHealth, health check handler
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleClean, data cleaning handler
func handleClean(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Sadece POST istekleri destekleniyor", http.StatusMethodNotAllowed)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Request body could not be read", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse JSON
	var req CleanRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "JSON parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Veriyi kontrol et
	if len(req.Data) == 0 {
		http.Error(w, "Data cannot be empty", http.StatusBadRequest)
		return
	}

	// Collect headers
	headers := make([]string, 0)
	headerMap := make(map[string]bool)

	// Iterate through all records to collect unique headers
	for _, record := range req.Data {
		for key := range record {
			if !headerMap[key] {
				headerMap[key] = true
				headers = append(headers, key)
			}
		}
	}

	// Convert data
	rows := make([][]string, len(req.Data))
	for i, record := range req.Data {
		row := make([]string, len(headers))
		for j, header := range headers {
			if val, ok := record[header]; ok {
				row[j] = fmt.Sprintf("%v", val)
			} else {
				row[j] = "" // For missing values, empty string
			}
		}
		rows[i] = row
	}

	// Create DataFrame
	df, err := cleaner.NewDataFrame(headers, rows)
	if err != nil {
		http.Error(w, "DataFrame creation error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create parallel processing options
	var parallelOptions []func(*cleaner.ParallelOptions)
	if req.MaxWorkers > 0 {
		parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(req.MaxWorkers))
	}

	// Apply cleanup actions
	for _, action := range req.Actions {
		parts := strings.Split(action, ":")
		actionType := parts[0]

		switch actionType {
		case "trim":
			if req.Parallel {
				df = df.TrimColumnsParallel(parallelOptions...)
			} else {
				df.TrimColumns()
			}
		case "normalize_dates":
			if len(parts) >= 2 {
				dateParts := strings.Split(parts[1], "=")
				if len(dateParts) == 2 {
					column, layout := dateParts[0], dateParts[1]
					var err error
					if req.Parallel {
						_, err = df.CleanDatesParallel(column, layout, parallelOptions...)
					} else {
						_, err = df.CleanDates(column, layout)
					}
					if err != nil {
						log.Printf("Tarih temizleme hatası: %v", err)
					}
				}
			}
		case "replace_nulls":
			if len(parts) >= 2 {
				nullParts := strings.Split(parts[1], "=")
				if len(nullParts) == 2 {
					column, value := nullParts[0], nullParts[1]
					var err error
					if req.Parallel {
						_, err = df.ReplaceNullsParallel(column, value, parallelOptions...)
					} else {
						_, err = df.ReplaceNulls(column, value)
					}
					if err != nil {
						log.Printf("Null değiştirme hatası: %v", err)
					}
				}
			}
		case "normalize_case":
			if len(parts) >= 2 {
				caseParts := strings.Split(parts[1], "=")
				if len(caseParts) == 2 {
					column, caseType := caseParts[0], caseParts[1]
					toUpper := strings.ToLower(caseType) == "upper"
					var err error
					if req.Parallel {
						_, err = df.NormalizeCaseParallel(column, toUpper, parallelOptions...)
					} else {
						_, err = df.NormalizeCase(column, toUpper)
					}
					if err != nil {
						log.Printf("Harf dönüşümü hatası: %v", err)
					}
				}
			}
		case "clean_regex":
			if len(parts) >= 2 {
				regexParts := strings.Split(parts[1], "=")
				if len(regexParts) == 3 {
					column, pattern, replacement := regexParts[0], regexParts[1], regexParts[2]
					var err error
					if req.Parallel {
						_, err = df.CleanWithRegexParallel(column, pattern, replacement, parallelOptions...)
					} else {
						_, err = df.CleanWithRegex(column, pattern, replacement)
					}
					if err != nil {
						log.Printf("Regex temizleme hatası: %v", err)
					}
				}
			}
		case "split_column":
			if len(parts) >= 2 {
				splitParts := strings.Split(parts[1], "=")
				if len(splitParts) >= 3 {
					column, separator := splitParts[0], splitParts[1]
					newColumns := strings.Split(splitParts[2], ",")
					if _, err := df.SplitColumn(column, separator, newColumns); err != nil {
						log.Printf("Sütun bölme hatası: %v", err)
					}
				}
			}
		case "filter_outliers":
			if len(parts) >= 2 {
				outlierParts := strings.Split(parts[1], "=")
				if len(outlierParts) == 3 {
					column := outlierParts[0]
					min, err1 := strconv.ParseFloat(outlierParts[1], 64)
					max, err2 := strconv.ParseFloat(outlierParts[2], 64)
					if err1 != nil || err2 != nil {
						log.Printf("Aykırı değer filtreleme hatası: Geçersiz sayı")
						continue
					}
					var err error
					if req.Parallel {
						_, err = df.FilterOutliersParallel(column, min, max, parallelOptions...)
					} else {
						_, err = df.FilterOutliers(column, min, max)
					}
					if err != nil {
						log.Printf("Aykırı değer filtreleme hatası: %v", err)
					}
				}
			}
		}
	}

	// Convert result
	result := make([]map[string]interface{}, len(df.GetData()))
	for i, row := range df.GetData() {
		record := make(map[string]interface{})
		for j, header := range df.GetHeaders() {
			if j < len(row) {
				record[header] = row[j]
			}
		}
		result[i] = record
	}

	// Prepare statistics
	rowCount, colCount := df.Shape()
	stats := map[string]int{
		"rows":    rowCount,
		"columns": colCount,
	}

	// Create response
	resp := CleanResponse{
		Data:       result,
		Statistics: stats,
		Message:    "Data cleaned successfully",
	}

	// Send JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// handleCleanFile, file cleaning handler
func handleCleanFile(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are supported", http.StatusMethodNotAllowed)
		return
	}

	// İstek gövdesini oku
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Request body could not be read", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// JSON'u parse et
	var req FileCleanRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "JSON parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Dosya yolunu kontrol et
	if req.FilePath == "" {
		http.Error(w, "File path not specified", http.StatusBadRequest)
		return
	}

	// Dosya formatını belirle
	inputFormat := getFileFormat(req.FilePath)
	if inputFormat == "" {
		http.Error(w, "Unsupported file format", http.StatusBadRequest)
		return
	}

	// Determine output file and format
	outputFile := req.Output
	outputFormat := req.Format

	if outputFile == "" {
		outputFile = "cleaned_" + filepath.Base(req.FilePath)
	}

	// If output format is not specified, use input format
	if outputFormat == "" {
		outputFormat = inputFormat
	}

	// Create parallel processing options
	var parallelOptions []func(*cleaner.ParallelOptions)
	if req.MaxWorkers > 0 {
		parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(req.MaxWorkers))
	}

	// Read data
	var df *cleaner.DataFrame
	var readErr error

	switch inputFormat {
	case "csv":
		df, readErr = cleaner.ReadCSV(req.FilePath)
	case "json":
		df, readErr = cleaner.ReadJSON(req.FilePath)
	case "excel":
		df, readErr = cleaner.ReadExcel(req.FilePath)
	case "parquet":
		df, readErr = cleaner.ReadParquet(req.FilePath)
	}

	if readErr != nil {
		http.Error(w, "Dosya okuma hatası: "+readErr.Error(), http.StatusInternalServerError)
		return
	}

	// Apply cleanup actions
	for _, action := range req.Actions {
		parts := strings.Split(action, ":")
		actionType := parts[0]

		switch actionType {
		case "trim":
			if req.Parallel {
				df = df.TrimColumnsParallel(parallelOptions...)
			} else {
				df.TrimColumns()
			}
		case "normalize_dates":
			if len(parts) >= 2 {
				dateParts := strings.Split(parts[1], "=")
				if len(dateParts) == 2 {
					column, layout := dateParts[0], dateParts[1]
					var err error
					if req.Parallel {
						_, err = df.CleanDatesParallel(column, layout, parallelOptions...)
					} else {
						_, err = df.CleanDates(column, layout)
					}
					if err != nil {
						log.Printf("Tarih temizleme hatası: %v", err)
					}
				}
			}
		case "replace_nulls":
			if len(parts) >= 2 {
				nullParts := strings.Split(parts[1], "=")
				if len(nullParts) == 2 {
					column, value := nullParts[0], nullParts[1]
					var err error
					if req.Parallel {
						_, err = df.ReplaceNullsParallel(column, value, parallelOptions...)
					} else {
						_, err = df.ReplaceNulls(column, value)
					}
					if err != nil {
						log.Printf("Null değiştirme hatası: %v", err)
					}
				}
			}
		case "normalize_case":
			if len(parts) >= 2 {
				caseParts := strings.Split(parts[1], "=")
				if len(caseParts) == 2 {
					column, caseType := caseParts[0], caseParts[1]
					toUpper := strings.ToLower(caseType) == "upper"
					var err error
					if req.Parallel {
						_, err = df.NormalizeCaseParallel(column, toUpper, parallelOptions...)
					} else {
						_, err = df.NormalizeCase(column, toUpper)
					}
					if err != nil {
						log.Printf("Harf dönüşümü hatası: %v", err)
					}
				}
			}
		case "clean_regex":
			if len(parts) >= 2 {
				regexParts := strings.Split(parts[1], "=")
				if len(regexParts) == 3 {
					column, pattern, replacement := regexParts[0], regexParts[1], regexParts[2]
					var err error
					if req.Parallel {
						_, err = df.CleanWithRegexParallel(column, pattern, replacement, parallelOptions...)
					} else {
						_, err = df.CleanWithRegex(column, pattern, replacement)
					}
					if err != nil {
						log.Printf("Regex temizleme hatası: %v", err)
					}
				}
			}
		case "split_column":
			if len(parts) >= 2 {
				splitParts := strings.Split(parts[1], "=")
				if len(splitParts) >= 3 {
					column, separator := splitParts[0], splitParts[1]
					newColumns := strings.Split(splitParts[2], ",")
					if _, err := df.SplitColumn(column, separator, newColumns); err != nil {
						log.Printf("Sütun bölme hatası: %v", err)
					}
				}
			}
		case "filter_outliers":
			if len(parts) >= 2 {
				outlierParts := strings.Split(parts[1], "=")
				if len(outlierParts) == 3 {
					column := outlierParts[0]
					min, err1 := strconv.ParseFloat(outlierParts[1], 64)
					max, err2 := strconv.ParseFloat(outlierParts[2], 64)
					if err1 != nil || err2 != nil {
						log.Printf("Aykırı değer filtreleme hatası: Geçersiz sayı")
						continue
					}
					var err error
					if req.Parallel {
						_, err = df.FilterOutliersParallel(column, min, max, parallelOptions...)
					} else {
						_, err = df.FilterOutliers(column, min, max)
					}
					if err != nil {
						log.Printf("Aykırı değer filtreleme hatası: %v", err)
					}
				}
			}
		}
	}

	// Write result
	var writeErr error
	switch outputFormat {
	case "csv":
		writeErr = df.WriteCSV(outputFile)
	case "json":
		writeErr = df.WriteJSON(outputFile)
	case "excel":
		writeErr = df.WriteExcel(outputFile)
	case "parquet":
		writeErr = df.WriteParquet(outputFile)
	}

	if writeErr != nil {
		http.Error(w, "Dosya yazma hatası: "+writeErr.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare statistics
	rowCount, colCount := df.Shape()
	stats := map[string]int{
		"rows":    rowCount,
		"columns": colCount,
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":    "File cleaned successfully",
		"output":     outputFile,
		"statistics": stats,
	})
}

// getFileFormat, file extension to format
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
