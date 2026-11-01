package search_test

import (
	"bytes"
	"encoding/json/v2"
	"testing"

	"github.com/postgresbase/dbx"
	"github.com/postgresbase/pocketbase/tools/search"
)

func TestMultiMatchSubqueryBuild(t *testing.T) {
	// create a dummy db
	db, cleanup := NewTestDBX()
	defer cleanup()

	mm := search.MultiMatchSubquery{
		TargetTableAlias: "test_TargetTableAlias",
		FromTableName:    "test_FromTableName",
		FromTableAlias:   "test_FromTableAlias",
		ValueIdentifier:  "({:mm},{:external})",
		Joins: []*search.Join{
			{TableName: "join_table1", TableAlias: "join_alias1"},
			{TableName: "join_table2", TableAlias: "join_alias2", On: dbx.NewExp("123={:join}", dbx.Params{"join": "test_join"})},
		},
		Params: dbx.Params{"mm": "test_mm"},
	}

	params := dbx.Params{"external": "test_external"}

	result := mm.Build(db, params)

	expectedResult := "SELECT ({:mm},{:external}) as [[multiMatchValue]] FROM \"test_FromTableName\" \"test_FromTableAlias\" LEFT JOIN \"join_table1\" \"join_alias1\" ON 1=1 LEFT JOIN \"join_table2\" \"join_alias2\" ON 123={:join} WHERE \"test_FromTableAlias\".\"id\" = \"test_TargetTableAlias\".\"id\""
	if expectedResult != result {
		t.Fatalf("Expected build result\n%v\ngot\n%v", expectedResult, result)
	}

	// the params from all expressions should be merged in the root
	rawParams, err := json.Marshal(params, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}

	expectedParams := []byte(`{"external":"test_external","join":"test_join","mm":"test_mm"}`)
	if !bytes.Equal(rawParams, expectedParams) {
		t.Fatalf("Expected final params\n%s\ngot\n%s", expectedParams, rawParams)
	}
}
