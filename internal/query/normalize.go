package query

import (
	"strings"
	"unicode/utf8"
)

const MaxQueryLen = 256

func Normalize(raw string) (string, bool) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return "", false
	}
	normalized = strings.ToLower(normalized)
	if strings.ContainsAny(normalized, " \t\n\r\v\f") {
		normalized = strings.Join(strings.Fields(normalized), " ")
	}
	if utf8.RuneCountInString(normalized) > MaxQueryLen {
		return "", false
	}
	return normalized, true
}
