package types

import "testing"

func TestColumnTypeConversion(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		expected ColumnType
	}{
		{"string type", "string", TypeString},
		{"int type", "int", TypeFloat},
		{"float type", "float", TypeFloat},
		{"unknown type", "unknown", TypeString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StringToColumnType(tt.typeStr)
			if got != tt.expected {
				t.Errorf("StringToColumnType(%q) = %v, want %v", tt.typeStr, got, tt.expected)
			}
		})
	}
}
