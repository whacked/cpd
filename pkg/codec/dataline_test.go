package codec_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/types"
)

type testCase struct {
	input    string
	expected types.ValuesWithColumns
}

var testRecords = []testCase{
	{
		`milk,234,123g`,
		types.ValuesWithColumns{
			Values: []interface{}{"milk", 234.0, "123g"},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeString},
			},
		},
	},
	{
		`water,456,456g`,
		types.ValuesWithColumns{
			Values: []interface{}{"water", 456.0, "456g"},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeString},
			},
		},
	},
	{
		`water,456,456g,,tap`,
		types.ValuesWithColumns{
			Values: []interface{}{"water", 456.0, "456g", nil, "tap"},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "milk", "water": "100g", "yield.grams": 120}`,
		types.ValuesWithColumns{
			Values: []interface{}{"milk", "100g", 120.0},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
			},
		},
	},
	{
		`{"category": "milk", "water": null, "yield.grams": 100}`,
		types.ValuesWithColumns{
			Values: []interface{}{"milk", nil, 100.0},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
			},
		},
	},
	{
		`["milk", "1g", 2]`,
		types.ValuesWithColumns{
			Values: []interface{}{"milk", "1g", 2.0},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
			},
		},
	},
	{
		`{"category": "coffee", "water": "120g", "yield.grams": 87, "origin": "kenya"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"coffee", "120g", 87.0, "kenya"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "origin", Type: types.TypeString},
			},
		},
	},
	{
		`["coffee", "120g", 63, "kenya"]`,
		types.ValuesWithColumns{
			Values: []interface{}{"coffee", "120g", 63.0, "kenya"},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "fail", "water": "120g", "yield.grams": 38, "roast": "pink"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"fail", "120g", 38.0, "pink"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "roast", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "coffee", "origin": "ethiopia", "water": "134g", "bean": "12.0g", "roast": "light", "yield.grams": 462, "notes": ["kiwi", "pastel", "butterfly"], "price": "33Gil"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"coffee", "ethiopia", "134g", "12.0g", "light", 462.0, []interface{}{"kiwi", "pastel", "butterfly"}, "33Gil"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "bean", Type: types.TypeString},
				{Name: "roast", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "notes", Type: types.TypeArray},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "coffee", "origin": "ethiopia", "water": "135g", "bean": "13.0g", "roast": "dark", "yield.grams": 267, "notes": ["kiwi", "pastel", "butterfly"], "price": "33Gil"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"coffee", "ethiopia", "135g", "13.0g", "dark", 267.0, []interface{}{"kiwi", "pastel", "butterfly"}, "33Gil"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "bean", Type: types.TypeString},
				{Name: "roast", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "notes", Type: types.TypeArray},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{ // index 11
		`{"category": "COFFEE", "origin": "Ethiopia", "water": "333g", "bean": "14.0g", "roast": "medium", "yield.grams": null, "notes": ["kiwi", "pastel", "butterfly"], "price": "33Gil"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"COFFEE", "Ethiopia", "333g", "14.0g", "medium", nil, []interface{}{"kiwi", "pastel", "butterfly"}, "33Gil"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "bean", Type: types.TypeString},
				{Name: "roast", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeString},
				{Name: "notes", Type: types.TypeArray},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "fail", "origin": "none", "water": "234g", "bean": "14.0g", "roast": "dark", "yield.grams": 126, "notes": ["kiwi", "pastel", "butterfly"], "price": "33Gil"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"fail", "none", "234g", "14.0g", "dark", 126.0, []interface{}{"kiwi", "pastel", "butterfly"}, "33Gil"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "bean", Type: types.TypeString},
				{Name: "roast", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "notes", Type: types.TypeArray},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "cOfFeE", "origin": "colombia", "water": "234g", "bean": "14.0g", "roast": "light", "yield.grams": 249, "notes": ["kiwi", "pastel", "butterfly"], "price": "33Gil"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"cOfFeE", "colombia", "234g", "14.0g", "light", 249.0, []interface{}{"kiwi", "pastel", "butterfly"}, "33Gil"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "bean", Type: types.TypeString},
				{Name: "roast", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "notes", Type: types.TypeArray},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "tea", "origin": "colombia", "water": "234g", "water.temperature": "96C", "yield.grams": 523, "price": "22Zeni"}`,
		types.ValuesWithColumns{
			Values: []interface{}{"tea", "colombia", "234g", "96C", 523.0, "22Zeni"},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "origin", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "water.temperature", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "price", Type: types.TypeString},
			},
		},
	},
	{
		`{"category": "coffee", "water": "120g", "yield.grams": 99, "origin": 1}`,
		types.ValuesWithColumns{
			Values: []interface{}{"coffee", "120g", 99.0, 1.0},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "origin", Type: types.TypeFloat},
			},
		},
	},
	{
		`{"category": "tea", "water": "120g", "yield.grams": 1, "origin": 2}`,
		types.ValuesWithColumns{
			Values: []interface{}{"tea", "120g", 1.0, 2.0},
			Columns: []types.ColumnInfo{
				{Name: "category", Type: types.TypeString},
				{Name: "water", Type: types.TypeString},
				{Name: "yield.grams", Type: types.TypeFloat},
				{Name: "origin", Type: types.TypeFloat},
			},
		},
	},
	{
		`["tea", "123g", 456, 2]`,
		types.ValuesWithColumns{
			Values: []interface{}{"tea", "123g", 456.0, 2.0},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeFloat},
			},
		},
	},
	{
		`[2, "99g", 22, 3]`,
		types.ValuesWithColumns{
			Values: []interface{}{2.0, "99g", 22.0, 3.0},
			Columns: []types.ColumnInfo{
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeString},
				{Name: "", Type: types.TypeFloat},
				{Name: "", Type: types.TypeFloat},
			},
		},
	},
}

func TestParseStrings(t *testing.T) {
	for i, tc := range testRecords {
		t.Run(fmt.Sprintf("Record %d", i), func(t *testing.T) {
			received, err := codec.ParseRecordToValuesWithColumns(tc.input)
			if err != nil {
				t.Fatalf("Failed to parse record: %v", err)
			}

			if !reflect.DeepEqual(received, tc.expected) {
				t.Errorf("\nInput: %s\nExpected: %+v\nReceived: %+v", tc.input, tc.expected, received)
				// Debug print the types
				t.Logf("Expected types:")
				for j, v := range tc.expected.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
				t.Logf("Received types:")
				for j, v := range received.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
			}
		})
	}
}
