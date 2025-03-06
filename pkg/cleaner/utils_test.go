package cleaner

import (
	"reflect"
	"testing"
	"time"
)

func TestTrimSpace(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"  test  ", "test"},
		{"test", "test"},
		{"", ""},
		{"  ", ""},
		{"\t\ntest\t\n", "test"},
	}

	for _, tt := range tests {
		result := trimSpace(tt.input)
		if result != tt.expected {
			t.Errorf("trimSpace(%q) = %q, expected = %q", tt.input, result, tt.expected)
		}
	}
}

func TestToUpperCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"test", "TEST"},
		{"Test", "TEST"},
		{"TEST", "TEST"},
		{"", ""},
		{"türkçe", "TÜRKÇE"},
	}

	for _, tt := range tests {
		result := toUpperCase(tt.input)
		if result != tt.expected {
			t.Errorf("toUpperCase(%q) = %q, expected = %q", tt.input, result, tt.expected)
		}
	}
}

func TestToLowerCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"TEST", "test"},
		{"Test", "test"},
		{"test", "test"},
		{"", ""},
		{"TÜRKÇE", "türkçe"},
	}

	for _, tt := range tests {
		result := toLowerCase(tt.input)
		if result != tt.expected {
			t.Errorf("toLowerCase(%q) = %q, expected = %q", tt.input, result, tt.expected)
		}
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		input       string
		expected    float64
		expectError bool
	}{
		{"123.45", 123.45, false},
		{"0", 0, false},
		{"-123.45", -123.45, false},
		{"", 0, true},
		{"abc", 0, true},
	}

	for _, tt := range tests {
		result, err := parseFloat(tt.input)
		if tt.expectError {
			if err == nil {
				t.Errorf("parseFloat(%q) expected error, but got none", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseFloat(%q) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("parseFloat(%q) = %v, expected = %v", tt.input, result, tt.expected)
			}
		}
	}
}

func TestCompileRegex(t *testing.T) {
	tests := []struct {
		pattern     string
		expectError bool
	}{
		{"[a-z]+", false},
		{"[0-9]+", false},
		{"[", true}, // Invalid regex
		{"(", true}, // Invalid regex
	}

	for _, tt := range tests {
		re, err := compileRegex(tt.pattern)
		if tt.expectError {
			if err == nil {
				t.Errorf("compileRegex(%q) expected error, but got none", tt.pattern)
			}
		} else {
			if err != nil {
				t.Errorf("compileRegex(%q) unexpected error: %v", tt.pattern, err)
			}
			if re == nil {
				t.Errorf("compileRegex(%q) returned nil, expected a regex object", tt.pattern)
			}
		}
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		input       string
		expected    time.Time
		expectError bool
	}{
		{"2006-01-02", time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), false},
		{"2006/01/02", time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), false},
		{"02-01-2006", time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), false},
		{"02/01/2006", time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), false},
		{"2006-01-02 15:04:05", time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), false},
		{"invalid", time.Time{}, true},
		{"", time.Time{}, true},
	}

	for _, tt := range tests {
		result, err := parseDate(tt.input, "2006-01-02")
		if tt.expectError {
			if err == nil {
				t.Errorf("parseDate(%q) expected error, but got none", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseDate(%q) unexpected error: %v", tt.input, err)
			}

			// Only compare year, month and day
			if result.Year() != tt.expected.Year() ||
				result.Month() != tt.expected.Month() ||
				result.Day() != tt.expected.Day() {
				t.Errorf("parseDate(%q) = %v, expected = %v", tt.input, result, tt.expected)
			}

			// If time is also specified, compare hour, minute and second
			if tt.input == "2006-01-02 15:04:05" {
				if result.Hour() != tt.expected.Hour() ||
					result.Minute() != tt.expected.Minute() ||
					result.Second() != tt.expected.Second() {
					t.Errorf("parseDate(%q) time part = %v, expected = %v", tt.input, result, tt.expected)
				}
			}
		}
	}
}

func TestSortInts(t *testing.T) {
	tests := []struct {
		input    []int
		expected []int
	}{
		{[]int{3, 1, 4, 2}, []int{1, 2, 3, 4}},
		{[]int{5, 5, 5, 5}, []int{5, 5, 5, 5}},
		{[]int{9, 8, 7, 6, 5}, []int{5, 6, 7, 8, 9}},
		{[]int{}, []int{}},
		{[]int{1}, []int{1}},
	}

	for _, tt := range tests {
		input := make([]int, len(tt.input))
		copy(input, tt.input)

		sortInts(input) // sortInts function modifies the value in place

		if !reflect.DeepEqual(input, tt.expected) {
			t.Errorf("sortInts(%v) = %v, expected = %v", tt.input, input, tt.expected)
		}
	}
}
