package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mstgnz/cleango/pkg/cleaner"
)

// CleanRequest, structure for cleanup request
type CleanRequest struct {
	Data       []map[string]interface{} `json:"data"`
	Actions    []string                 `json:"actions"`
	Format     string                   `json:"format,omitempty"`
	Parallel   bool                     `json:"parallel,omitempty"`
	MaxWorkers int                      `json:"max_workers,omitempty"`
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
	Format     string   `json:"format,omitempty"`
	Output     string   `json:"output,omitempty"`
	Parallel   bool     `json:"parallel,omitempty"`
	MaxWorkers int      `json:"max_workers,omitempty"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/clean", handleClean)
	mux.HandleFunc("/clean-file", handleCleanFile)
	mux.HandleFunc("/health", handleHealth)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("CleanGo API starting on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server stopped")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("failed to encode JSON response: %v", err)
	}
}

// handleHealth, health check handler
func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleClean, data cleaning handler
func handleClean(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are supported", http.StatusMethodNotAllowed)
		return
	}

	var req CleanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON parse error: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(req.Data) == 0 {
		http.Error(w, "Data cannot be empty", http.StatusBadRequest)
		return
	}

	// Collect headers (preserve first-seen order)
	headers := make([]string, 0)
	headerMap := make(map[string]bool)
	for _, record := range req.Data {
		for key := range record {
			if !headerMap[key] {
				headerMap[key] = true
				headers = append(headers, key)
			}
		}
	}

	rows := make([][]string, len(req.Data))
	for i, record := range req.Data {
		row := make([]string, len(headers))
		for j, header := range headers {
			if val, ok := record[header]; ok {
				row[j] = fmt.Sprintf("%v", val)
			}
		}
		rows[i] = row
	}

	df, err := cleaner.NewDataFrame(headers, rows)
	if err != nil {
		http.Error(w, "DataFrame creation error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	parallelOptions := []func(*cleaner.ParallelOptions){
		cleaner.WithContext(r.Context()),
	}
	if req.MaxWorkers > 0 {
		parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(req.MaxWorkers))
	}

	if err := applyActions(df, req.Actions, req.Parallel, parallelOptions); err != nil {
		http.Error(w, "Action error: "+err.Error(), http.StatusBadRequest)
		return
	}

	result := dataFrameToMaps(df)
	rowCount, colCount := df.Shape()

	writeJSON(w, http.StatusOK, CleanResponse{
		Data:       result,
		Statistics: map[string]int{"rows": rowCount, "columns": colCount},
		Message:    "Data cleaned successfully",
	})
}

// handleCleanFile, file cleaning handler
func handleCleanFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are supported", http.StatusMethodNotAllowed)
		return
	}

	var req FileCleanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON parse error: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if req.FilePath == "" {
		http.Error(w, "File path not specified", http.StatusBadRequest)
		return
	}

	// Prevent path traversal
	cleanPath := filepath.Clean(req.FilePath)
	absPath, err := filepath.Abs(cleanPath)
	if err != nil || strings.Contains(absPath, "..") {
		http.Error(w, "Invalid file path", http.StatusBadRequest)
		return
	}

	// Restrict to current working directory or a dedicated data dir
	workDir, _ := os.Getwd()
	if !strings.HasPrefix(absPath, workDir) {
		http.Error(w, "File path is outside the allowed directory", http.StatusForbidden)
		return
	}

	inputFormat := getFileFormat(req.FilePath)
	if inputFormat == "" {
		http.Error(w, "Unsupported file format", http.StatusBadRequest)
		return
	}

	outputFile := req.Output
	outputFormat := req.Format

	if outputFile == "" {
		outputFile = "cleaned_" + filepath.Base(req.FilePath)
	}
	if outputFormat == "" {
		outputFormat = inputFormat
	}

	var parallelOptions []func(*cleaner.ParallelOptions)
	if req.MaxWorkers > 0 {
		parallelOptions = append(parallelOptions, cleaner.WithMaxWorkers(req.MaxWorkers))
	}

	var df *cleaner.DataFrame
	switch inputFormat {
	case "csv":
		df, err = cleaner.ReadCSV(req.FilePath)
	case "json":
		df, err = cleaner.ReadJSON(req.FilePath)
	case "excel":
		df, err = cleaner.ReadExcel(req.FilePath)
	case "parquet":
		df, err = cleaner.ReadParquet(req.FilePath)
	}
	if err != nil {
		http.Error(w, "File read error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := applyActions(df, req.Actions, req.Parallel, parallelOptions); err != nil {
		http.Error(w, "Action error: "+err.Error(), http.StatusBadRequest)
		return
	}

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
		http.Error(w, "File write error: "+writeErr.Error(), http.StatusInternalServerError)
		return
	}

	rowCount, colCount := df.Shape()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":    "File cleaned successfully",
		"output":     outputFile,
		"statistics": map[string]int{"rows": rowCount, "columns": colCount},
	})
}

// applyActions applies the list of cleaning actions to the DataFrame.
func applyActions(df *cleaner.DataFrame, actions []string, parallel bool, parallelOptions []func(*cleaner.ParallelOptions)) error {
	for _, action := range actions {
		parts := strings.SplitN(action, ":", 2)
		actionType := parts[0]

		switch actionType {
		case "trim":
			if parallel {
				if trimmed, err := df.TrimColumnsParallel(parallelOptions...); err != nil {
					return err
				} else {
					df = trimmed
				}
			} else {
				df.TrimColumns()
			}

		case "normalize_dates":
			if len(parts) < 2 {
				continue
			}
			dateParts := strings.SplitN(parts[1], "=", 2)
			if len(dateParts) != 2 {
				continue
			}
			column, layout := dateParts[0], dateParts[1]
			var err error
			if parallel {
				_, err = df.CleanDatesParallel(column, layout, parallelOptions...)
			} else {
				_, err = df.CleanDates(column, layout)
			}
			if err != nil {
				log.Printf("normalize_dates error: %v", err)
			}

		case "replace_nulls":
			if len(parts) < 2 {
				continue
			}
			nullParts := strings.SplitN(parts[1], "=", 2)
			if len(nullParts) != 2 {
				continue
			}
			column, value := nullParts[0], nullParts[1]
			var err error
			if parallel {
				_, err = df.ReplaceNullsParallel(column, value, parallelOptions...)
			} else {
				_, err = df.ReplaceNulls(column, value)
			}
			if err != nil {
				log.Printf("replace_nulls error: %v", err)
			}

		case "normalize_case":
			if len(parts) < 2 {
				continue
			}
			caseParts := strings.SplitN(parts[1], "=", 2)
			if len(caseParts) != 2 {
				continue
			}
			column, caseType := caseParts[0], caseParts[1]
			toUpper := strings.ToLower(caseType) == "upper"
			var err error
			if parallel {
				_, err = df.NormalizeCaseParallel(column, toUpper, parallelOptions...)
			} else {
				_, err = df.NormalizeCase(column, toUpper)
			}
			if err != nil {
				log.Printf("normalize_case error: %v", err)
			}

		case "clean_regex":
			if len(parts) < 2 {
				continue
			}
			regexParts := strings.SplitN(parts[1], "=", 3)
			if len(regexParts) != 3 {
				continue
			}
			column, pattern, replacement := regexParts[0], regexParts[1], regexParts[2]
			var err error
			if parallel {
				_, err = df.CleanWithRegexParallel(column, pattern, replacement, parallelOptions...)
			} else {
				_, err = df.CleanWithRegex(column, pattern, replacement)
			}
			if err != nil {
				return fmt.Errorf("clean_regex error: %w", err)
			}

		case "split_column":
			if len(parts) < 2 {
				continue
			}
			splitParts := strings.SplitN(parts[1], "=", 3)
			if len(splitParts) < 3 {
				continue
			}
			column, separator := splitParts[0], splitParts[1]
			newColumns := strings.Split(splitParts[2], ",")
			if _, err := df.SplitColumn(column, separator, newColumns); err != nil {
				log.Printf("split_column error: %v", err)
			}

		case "filter_outliers":
			if len(parts) < 2 {
				continue
			}
			outlierParts := strings.SplitN(parts[1], "=", 3)
			if len(outlierParts) != 3 {
				continue
			}
			column := outlierParts[0]
			min, err1 := strconv.ParseFloat(outlierParts[1], 64)
			max, err2 := strconv.ParseFloat(outlierParts[2], 64)
			if err1 != nil || err2 != nil {
				log.Printf("filter_outliers: invalid number")
				continue
			}
			var err error
			if parallel {
				_, err = df.FilterOutliersParallel(column, min, max, parallelOptions...)
			} else {
				_, err = df.FilterOutliers(column, min, max)
			}
			if err != nil {
				log.Printf("filter_outliers error: %v", err)
			}
		}
	}
	return nil
}

func dataFrameToMaps(df *cleaner.DataFrame) []map[string]interface{} {
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
	return result
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
