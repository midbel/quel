package quel

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	data := []struct {
		Options []UpdateOption
		Table   string
		Want    string
		Args    []interface{}
	}{
		{
			Options: []UpdateOption{
				UpdateColumn("role", NewLiteral("test")),
			},
			Table: "users",
			Want:  "UPDATE users SET role = 'test'",
		},
		{
			Options: []UpdateOption{
				UpdateColumn("role", NewLiteral("test")),
				UpdateColumn("active", NewLiteral(1)),
				UpdateWhere(Equal(NewIdent("active"), Arg("active", 0))),
			},
			Table: "users",
			Want:  "UPDATE users SET role = 'test', active = 1 WHERE active = ?",
			Args:  []interface{}{0},
		},
	}
	for _, d := range data {
		q, err := NewUpdate(d.Table, d.Options...)
		if err != nil {
			t.Errorf("error creating query! %s", err)
			continue
		}
		compareQueries(t, q, d.Want, d.Args)
	}
}
