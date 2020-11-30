package quel

import (
	"testing"
)

func TestDelete(t *testing.T) {
	options := []SelectOption{
		SelectColumn(Func("AVG", NewIdent("conn"))),
		SelectGroupBy(NewIdent("id")),
	}
	query, _ := NewSelect("users", options...)
	data := []struct {
		Options []DeleteOption
		Table   string
		Want    string
		Args    []interface{}
	}{
		{
			Table: "users",
			Want:  "DELETE FROM users",
		},
		{
			Options: []DeleteOption{
				DeleteWhere(Equal(NewIdent("role"), NewLiteral("admin"))),
			},
			Table: "users",
			Want:  "DELETE FROM users WHERE role = 'admin'",
		},
		{
			Options: []DeleteOption{
				DeleteAlias("u"),
				DeleteWhere(NotEqual(NewIdent("role", "u"), Arg("role", "test"))),
			},
			Table: "users",
			Want:  "DELETE FROM users AS u WHERE u.role <> @role",
			Args:  []interface{}{"test"},
		},
		{
			Options: []DeleteOption{
				DeleteWhere(And(GreaterOrEqual(NewIdent("conn"), query), NotEqual(NewIdent("role"), Arg("role", "test")))),
			},
			Table: "users",
			Want:  "DELETE FROM users WHERE conn >= (SELECT AVG(conn) FROM users GROUP BY id) AND role <> @role",
			Args:  []interface{}{"test"},
		},
	}
	for _, d := range data {
		q, err := NewDelete(d.Table, d.Options...)
		if err != nil {
			t.Errorf("error creating query! %s", err)
			continue
		}
		compareQueries(t, q, d.Want, d.Args)
	}
}
