package quel

import (
	"fmt"
	"strings"
)

type SelectOption func(q *Select) error

func SelectLimit(limit int) SelectOption {
	return func(q *Select) error {
		if limit <= 0 {
			return fmt.Errorf("limit: %w: %s", ErrLimit, limit)
		}
		q.limit = limit
		return nil
	}
}

func SelectOffset(offset int) SelectOption {
	return func(q *Select) error {
		if offset <= 0 {
			return fmt.Errorf("offset: %w: %s", ErrLimit, offset)
		}
		q.offset = offset
		return nil
	}
}

func SelectColumns(columns ...string) SelectOption {
	return func(q *Select) error {
		var (
			cs = make([]SQLer, len(columns))
			nq = len(q.queries) - 1
		)
		for i := range columns {
			if !isValidIdentifier(columns[i]) {
				return fmt.Errorf("column: %w %q", ErrIdent, columns[i])
			}
			cs[i] = NewIdent(columns[i])
		}
		q.queries[nq].columns = append(q.queries[nq].columns, cs...)
		return nil
	}
}

func SelectColumn(sql SQLer) SelectOption {
	return func(q *Select) error {
		n := len(q.queries) - 1
		if n >= 0 {
			q.queries[n].columns = append(q.queries[n].columns, sql)
		}
		return nil
	}
}

func SelectAlias(name string) SelectOption {
	return func(q *Select) error {
		if !isValidIdentifier(name) {
			return fmt.Errorf("alias: %w %q", ErrIdent, name)
		}
		q.queries[0].table = Alias(name, q.queries[0].table)
		return nil
	}
}

func SelectOrderBy(by ...SQLer) SelectOption {
	return func(q *Select) error {
		for i := range by {
			o, ok := by[i].(orderby)
			if !ok || !isValidIdentifier(o.column) {
				return fmt.Errorf("ORDER BY: %w %q", ErrIdent, o.column)
			}
		}
		q.orderby = append(q.orderby, by...)
		return nil
	}
}

func SelectGroupBy(columns ...string) SelectOption {
	return func(q *Select) error {
		for i := range columns {
			if !isValidIdentifier(columns[i]) {
				return fmt.Errorf("GROUP BY: %w %q", ErrIdent, columns[i])
			}
		}
		q.groupby = append(q.groupby, columns...)
		return nil
	}
}

func SelectWhere(where SQLer) SelectOption {
	return func(q *Select) error {
		if where == nil {
			return nil
		}
		if !acceptRelational(where) {
			return fmt.Errorf("where: %w", ErrSyntax)
		}
		q.where = where
		return nil
	}
}

type jointype uint8

const (
	none jointype = iota
	innerLeft
	innerRight
	outerLeft
	outerRight
)

var joinops = map[jointype]string{
	none:       "",
	innerLeft:  "LEFT INNER JOIN",
	innerRight: "RIGHT INNER JOIN",
	outerLeft:  "LEFT OUTER JOIN",
	outerRight: "RIGHT OUTER JOIN",
}

type query struct {
	columns []SQLer
	table   SQLer
	cdt     SQLer
	join    jointype
}

func isJoinable(sql SQLer) bool {
	switch sql := sql.(type) {
	case Select, ident:
		return true
	case alias:
		return isJoinable(sql.SQLer)
	default:
		return false
	}
}

type Select struct {
	queries []query
	orderby []SQLer
	groupby []string
	limit   int
	offset  int
	where   SQLer
}

func NewSelect(table string, options ...SelectOption) (Select, error) {
	var (
		base Select
		err  error
		q    query
	)
	q.table = NewIdent(table)
	base.queries = append(base.queries, q)

	for _, opt := range options {
		if err = opt(&base); err != nil {
			break
		}
	}
	return base, err
}

func (s Select) LeftInnerJoin(source, cdt SQLer, options ...SelectOption) (Select, error) {
	return s.join(innerLeft, source, cdt, options...)
}

func (s Select) RightInnerJoin(source, cdt SQLer, options ...SelectOption) (Select, error) {
	return s.join(innerRight, source, cdt, options...)
}

func (s Select) LeftOuterJoin(source, cdt SQLer, options ...SelectOption) (Select, error) {
	return s.join(outerLeft, source, cdt, options...)
}

func (s Select) RightOuterJoin(source, cdt SQLer, options ...SelectOption) (Select, error) {
	return s.join(outerRight, source, cdt, options...)
}

func (s Select) join(jt jointype, source, cdt SQLer, options ...SelectOption) (Select, error) {
	if cdt != nil {
		switch cdt.(type) {
		case compare, and, or, list:
		default:
			return s, fmt.Errorf("%w: invalid condition type", ErrSyntax)
		}
	}
	var (
		base Select
		err  error
		q    query
	)
	q.table = source
	q.cdt = cdt
	q.join = jt

	base = Select{
		queries: append([]query{}, s.queries...),
	}
	base.queries = append(base.queries, q)
	for _, opt := range options {
		if err = opt(&base); err != nil {
			break
		}
	}
	return base, err
}

func (s Select) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("SELECT ")
	for i, q := range s.queries {
		if len(q.columns) == 0 {
			b.WriteString("*")
		}
		for j, c := range q.columns {
			if i > 0 || j > 0 {
				b.WriteString(", ")
			}
			sql, as, err := c.SQL()
			if err != nil {
				return "", nil, err
			}
			args = append(args, as...)
			b.WriteString(sql)
		}
	}
	b.WriteString(" FROM ")
	for i, q := range s.queries {
		if i > 0 && q.join != none {
			b.WriteString(" ")
			b.WriteString(joinops[q.join])
			b.WriteString(" ")
		}
		sql, as, err := q.table.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
		switch q.cdt.(type) {
		case and, or, compare:
			b.WriteString(" ON ")
			sql, as, err := q.cdt.SQL()
			if err != nil {
				return "", nil, err
			}
			args = append(args, as...)
			b.WriteString(sql)
		case list:
			b.WriteString(" USING (")
			sql, as, err := q.cdt.SQL()
			if err != nil {
				return "", nil, err
			}
			args = append(args, as...)
			b.WriteString(sql)
			b.WriteString(")")
		default:
		}
	}
	if s.where != nil {
		sql, as, err := s.where.SQL()
		if err != nil {
			return "", nil, err
		}
		b.WriteString(" WHERE ")
		b.WriteString(sql)
		args = append(args, as...)
	}
	return b.String(), args, nil
}

type groupby struct {
	column string
	having SQLer
}

func GroupBy(column string, having SQLer) SQLer {
	return groupby{
		column: column,
		having: having,
	}
}

func (g groupby) SQL() (string, []interface{}, error) {
	return "", nil, nil
}

type orderby struct {
	column string
	order  string
}

func (o orderby) SQL() (string, []interface{}, error) {
	return fmt.Sprintf("%s %s", o.column, o.order), nil, nil
}

func Asc(column string) SQLer {
	return orderby{
		column: column,
		order:  "ASC",
	}
}

func Desc(column string) SQLer {
	return orderby{
		column: column,
		order:  "DESC",
	}
}
