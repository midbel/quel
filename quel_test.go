package quel

import (
	"reflect"
	"testing"
)

func compareQueries(t *testing.T, q SQLer, sql string, args []interface{}) {
	t.Helper()
	str, as, err := q.SQL()
	if err != nil {
		t.Errorf("%s: error when building query: %s", sql, err)
		return
	}
	if str != sql {
		t.Errorf("queries mismatched!")
		t.Logf("\twant: %s", sql)
		t.Logf("\tgot:  %s", str)
		return
	}
	if !reflect.DeepEqual(as, args) {
		t.Errorf("arguments mismatched!")
		t.Logf("\twant: %v", args)
		t.Logf("\tgot:  %v", as)
	}
}
