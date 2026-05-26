package query

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		input  string
		want   string
		wantOK bool
	}{
		{"golang", "golang", true},
		{"  golang  ", "golang", true},
		{"Go Lang", "go lang", true},
		{"  Go   Lang  ", "go lang", true},
		{"", "", false},
		{"   ", "", false},
		{"iPhone\t15", "iphone 15", true},
		{"iPhone\n15", "iphone 15", true},
		{repeat("а", MaxQueryLen), repeat("а", MaxQueryLen), true},
		{repeat("а", MaxQueryLen+1), "", false},
	}

	for _, tt := range tests {
		got, ok := Normalize(tt.input)
		if ok != tt.wantOK {
			t.Errorf("Normalize(%q): ok=%v, want %v", tt.input, ok, tt.wantOK)
			continue
		}
		if ok && got != tt.want {
			t.Errorf("Normalize(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
