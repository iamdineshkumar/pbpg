package dbutils_test

import (
	"testing"

	"github.com/postgresbase/pocketbase/tools/dbutils"
)

func TestJSONEach(t *testing.T) {
	result := dbutils.JSONEach("a.b")

	expected := "jsonb_array_elements_text(CASE WHEN ([[a.b]] IS JSON OR json_valid([[a.b]]::text)) AND jsonb_typeof([[a.b]]::jsonb) = 'array' THEN [[a.b]]::jsonb ELSE jsonb_build_array([[a.b]]) END)"

	if result != expected {
		t.Fatalf("Expected\n%v\ngot\n%v", expected, result)
	}
}

func TestJSONArrayLength(t *testing.T) {
	result := dbutils.JSONArrayLength("a.b")

	expected := "(CASE WHEN ([[a.b]] IS JSON OR JSON_VALID([[a.b]]::text)) AND jsonb_typeof([[a.b]]::jsonb) = 'array' THEN jsonb_array_length([[a.b]]::jsonb) ELSE 0 END)"

	if result != expected {
		t.Fatalf("Expected\n%v\ngot\n%v", expected, result)
	}
}

func TestJSONExtract(t *testing.T) {
	scenarios := []struct {
		name     string
		column   string
		path     string
		expected string
	}{
		{
			"empty path",
			"a.b",
			"",
			"JSON_QUERY_OR_NULL([[a.b]], '$')::jsonb",
		},
		{
			"starting with array index",
			"a.b",
			"[1].a[2]",
			"JSON_QUERY_OR_NULL([[a.b]], '$[1].a[2]')::jsonb",
		},
		{
			"starting with key",
			"a.b",
			"a.b[2].c",
			"JSON_QUERY_OR_NULL([[a.b]], '$.a.b[2].c')::jsonb",
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			result := dbutils.JSONExtract(s.column, s.path)

			if result != s.expected {
				t.Fatalf("Expected\n%v\ngot\n%v", s.expected, result)
			}
		})
	}
}
