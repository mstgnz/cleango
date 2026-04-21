package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mstgnz/cleango/pkg/cleaner"
)

func TestHandleHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", resp["status"])
	}
}

func TestHandleClean_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/clean", nil)
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleClean_EmptyData(t *testing.T) {
	body := `{"data":[],"actions":["trim"]}`
	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleClean_InvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBufferString("not json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleClean_TrimAction(t *testing.T) {
	payload := CleanRequest{
		Data: []map[string]interface{}{
			{"name": "  Alice  ", "age": "30"},
			{"name": "  Bob  ", "age": "25"},
		},
		Actions: []string{"trim"},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp CleanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 records, got %d", len(resp.Data))
	}

	for _, record := range resp.Data {
		name, ok := record["name"].(string)
		if !ok {
			t.Fatal("name field missing or wrong type")
		}
		if name == "  Alice  " || name == "  Bob  " {
			t.Errorf("trim did not remove spaces: %q", name)
		}
	}
}

func TestHandleClean_NormalizeCase(t *testing.T) {
	payload := CleanRequest{
		Data: []map[string]interface{}{
			{"city": "istanbul"},
		},
		Actions: []string{"normalize_case:city=upper"},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp CleanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if city, ok := resp.Data[0]["city"].(string); !ok || city != "ISTANBUL" {
		t.Errorf("expected city=ISTANBUL, got %v", resp.Data[0]["city"])
	}
}

func TestHandleClean_ReplaceNulls(t *testing.T) {
	payload := CleanRequest{
		Data: []map[string]interface{}{
			{"name": "Alice", "score": ""},
			{"name": "Bob", "score": "95"},
		},
		Actions: []string{"replace_nulls:score=0"},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp CleanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	for _, record := range resp.Data {
		if record["name"] == "Alice" {
			if record["score"] != "0" {
				t.Errorf("expected score=0 for Alice, got %v", record["score"])
			}
		}
	}
}

func TestHandleClean_CleanRegex(t *testing.T) {
	payload := CleanRequest{
		Data: []map[string]interface{}{
			{"phone": "555-123-4567"},
		},
		Actions: []string{"clean_regex:phone=[^0-9]="},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp CleanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if phone, ok := resp.Data[0]["phone"].(string); !ok || phone != "5551234567" {
		t.Errorf("expected phone=5551234567, got %v", resp.Data[0]["phone"])
	}
}

func TestHandleClean_StatisticsReturned(t *testing.T) {
	payload := CleanRequest{
		Data: []map[string]interface{}{
			{"a": "1", "b": "2"},
			{"a": "3", "b": "4"},
			{"a": "5", "b": "6"},
		},
		Actions: []string{},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/clean", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleClean(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp CleanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Statistics["rows"] != 3 {
		t.Errorf("expected rows=3, got %d", resp.Statistics["rows"])
	}
	if resp.Statistics["columns"] != 2 {
		t.Errorf("expected columns=2, got %d", resp.Statistics["columns"])
	}
}

func TestHandleCleanFile_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/clean-file", nil)
	w := httptest.NewRecorder()

	handleCleanFile(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleCleanFile_EmptyFilePath(t *testing.T) {
	body := `{"file_path":""}`
	req := httptest.NewRequest(http.MethodPost, "/clean-file", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleCleanFile(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleCleanFile_PathTraversal(t *testing.T) {
	body := `{"file_path":"../../etc/passwd"}`
	req := httptest.NewRequest(http.MethodPost, "/clean-file", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleCleanFile(w, req)

	// Should be rejected (403 forbidden or 400 bad request)
	if w.Code == http.StatusOK {
		t.Error("path traversal should have been rejected")
	}
}

func TestHandleCleanFile_UnsupportedFormat(t *testing.T) {
	body := `{"file_path":"testdata/file.txt"}`
	req := httptest.NewRequest(http.MethodPost, "/clean-file", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleCleanFile(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unsupported format, got %d", w.Code)
	}
}

func TestGetFileFormat(t *testing.T) {
	tests := []struct {
		path   string
		expect string
	}{
		{"data.csv", "csv"},
		{"data.CSV", "csv"},
		{"data.json", "json"},
		{"data.JSON", "json"},
		{"data.xlsx", "excel"},
		{"data.xls", "excel"},
		{"data.parquet", "parquet"},
		{"data.txt", ""},
		{"data", ""},
	}

	for _, tt := range tests {
		got := getFileFormat(tt.path)
		if got != tt.expect {
			t.Errorf("getFileFormat(%q) = %q, want %q", tt.path, got, tt.expect)
		}
	}
}

func TestApplyActions_UnknownAction(t *testing.T) {
	df, err := cleaner.NewDataFrame([]string{"name"}, [][]string{{"Alice"}})
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	if err := applyActions(df, []string{"unknown_action:foo=bar"}, false, nil); err != nil {
		t.Errorf("unknown action should be ignored, got error: %v", err)
	}
}

func TestApplyActions_ParallelTrim(t *testing.T) {
	df, err := cleaner.NewDataFrame([]string{"name"}, [][]string{{"  Alice  "}})
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	if err := applyActions(df, []string{"trim"}, true, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if df.GetData()[0][0] != "Alice" {
		t.Errorf("expected Alice, got %q", df.GetData()[0][0])
	}
}
