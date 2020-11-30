package quel

import (
	"testing"
)

func TestSelect(t *testing.T) {
	t.Run("simple", testSimpleSelect)
	t.Run("join", testJoinSelect)
	t.Run("subquery", testSubquerySelect)
}

func testSubquerySelect(t *testing.T) {
	t.SkipNow()
}

func testJoinSelect(t *testing.T) {
	qu, err := joinUsers()
	if err != nil {
		t.Errorf("error while creating users query! %s", err)
		return
	}
	const (
		ileft  = "SELECT u.id, u.first, u.last, p.id, p.name FROM users AS u LEFT INNER JOIN positions AS p ON u.id = p.user"
		iright = "SELECT u.id, u.first, u.last, p.id, p.name FROM users AS u RIGHT INNER JOIN positions AS p ON u.id = p.user"
		oleft  = "SELECT u.id, u.first, u.last, p.id, p.name FROM users AS u LEFT OUTER JOIN positions AS p ON u.id = p.user"
		oright = "SELECT u.id, u.first, u.last, p.id, p.name FROM users AS u RIGHT OUTER JOIN positions AS p ON u.id = p.user"
	)
	var (
		options = []SelectOption{
			SelectColumn(NewIdent("id", "p")),
			SelectColumn(NewIdent("name", "p")),
		}
		predicate = Equal(NewIdent("id", "u"), NewIdent("user", "p"))
		query     Select
		source    = Alias("p", NewIdent("positions"))
	)
	query, err = qu.LeftInnerJoin(source, predicate, options...)
	compareQueries(t, query, ileft, nil)

	query, err = qu.RightInnerJoin(source, predicate, options...)
	compareQueries(t, query, iright, nil)

	query, err = qu.LeftOuterJoin(source, predicate, options...)
	compareQueries(t, query, oleft, nil)

	query, err = qu.RightOuterJoin(source, predicate, options...)
	compareQueries(t, query, oright, nil)
}

func joinUsers() (Select, error) {
	options := []SelectOption{
		SelectColumn(NewIdent("id", "u")),
		SelectColumn(NewIdent("first", "u")),
		SelectColumn(NewIdent("last", "u")),
		SelectAlias("u"),
	}
	return NewSelect("users", options...)
}

func joinPositions() (Select, error) {
	options := []SelectOption{
		SelectColumn(NewIdent("id", "p")),
		SelectColumn(NewIdent("name", "p")),
		SelectColumn(NewIdent("admin", "p")),
		SelectAlias("p"),
	}
	return NewSelect("positions", options...)
}

func testSimpleSelect(t *testing.T) {
	data := []struct {
		Options []SelectOption
		Table   string
		Want    string
		Args    []interface{}
	}{
		{
			Options: []SelectOption{},
			Table:   "users",
			Want:    "SELECT * FROM users",
		},
		{
			Options: []SelectOption{
				SelectColumns("id", "first", "last"),
				SelectLimit(10),
			},
			Table: "users",
			Want:  "SELECT id, first, last FROM users LIMIT 10",
		},
		{
			Options: []SelectOption{
				SelectColumn(NewIdent("id", "u")),
				SelectColumn(NewIdent("first", "u")),
				SelectColumn(NewIdent("last", "u")),
				SelectOrderBy(Asc("first"), Asc("last")),
				SelectAlias("u"),
				SelectWhere(Equal(NewIdent("role"), Arg("role", "admin"))),
			},
			Table: "users",
			Want:  "SELECT u.id, u.first, u.last FROM users AS u WHERE role = ? ORDER BY first ASC, last ASC",
			Args:  []interface{}{"admin"},
		},
		{
			Options: []SelectOption{
				SelectGroupBy(NewIdent("active")),
				SelectColumn(Func("COUNT", NewIdent("id"))),
			},
			Table: "users",
			Want:  "SELECT COUNT(id) FROM users GROUP BY active",
		},
	}
	for _, d := range data {
		q, err := NewSelect(d.Table, d.Options...)
		if err != nil {
			t.Errorf("error creating query! %s", err)
			continue
		}
		compareQueries(t, q, d.Want, d.Args)
	}
}
