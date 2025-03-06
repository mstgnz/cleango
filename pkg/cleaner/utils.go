package cleaner

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrColumnNotFound is the error returned when a column is not found
var ErrColumnNotFound = errors.New("column not found")

// trimSpace removes whitespace from the beginning and end of a string
func trimSpace(s string) string {
	return strings.TrimSpace(s)
}

// toUpperCase converts a string to uppercase
func toUpperCase(s string) string {
	return strings.ToUpper(s)
}

// toLowerCase converts a string to lowercase
func toLowerCase(s string) string {
	return strings.ToLower(s)
}

// parseFloat converts a string to float64
func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}

// compileRegex compiles a regex pattern
func compileRegex(pattern string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}
	return re, nil
}

// parseDate converts a string to time.Time
func parseDate(s string, layout string) (time.Time, error) {
	// Try different formats
	formats := []string{
		layout,
		"2006-01-02",
		"2006/01/02",
		"02-01-2006",
		"02/01/2006",
		"01-02-2006",
		"01/02/2006",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"01-02-2006 15:04:05",
		"01/02/2006 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
		time.RFC1123,
		time.RFC1123Z,
		time.RFC822,
		time.RFC822Z,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
	}

	for _, format := range formats {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse date: %s", s)
}

// sortInts sorts an int slice
func sortInts(a []int) {
	sort.Ints(a)
}
