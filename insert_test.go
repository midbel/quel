package quel

import (
	"testing"
)

func TestInsert(t *testing.T) {
	data := []struct {
		Options []InsertOption
		Table   string
		Want    string
		Args    []interface{}
	}{
		{
			Options: []InsertOption{
				InsertValues(NewIdent("default"), NewLiteral("roger"), NewLiteral("lamotte")),
			},
			Table: "users",
			Want:  "INSERT INTO users VALUES (default, 'roger', 'lamotte')",
		},
		{
			Options: []InsertOption{
				InsertColumns("first", "last"),
				InsertValues(NewLiteral("roger"), NewLiteral("lamotte")),
			},
			Table: "users",
			Want:  "INSERT INTO users(first, last) VALUES ('roger', 'lamotte')",
		},
		{
			Options: []InsertOption{
				InsertColumns("first", "last"),
				InsertValues(NewLiteral("roger"), NewLiteral("lamotte")),
				InsertValues(Arg("first", "pierre"), Arg("last", "dubois")),
			},
			Table: "users",
			Want:  "INSERT INTO users(first, last) VALUES ('roger', 'lamotte'), (?, ?)",
			Args:  []interface{}{"pierre", "dubois"},
		},
	}
	for _, d := range data {
		q, err := NewInsert(d.Table, d.Options...)
		if err != nil {
			t.Errorf("error creating query! %s", err)
			continue
		}
		compareQueries(t, q, d.Want, d.Args)
	}
}
