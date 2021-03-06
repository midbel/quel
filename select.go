package quel

import (
	"fmt"
	"strconv"
	"strings"
)

type SelectOption func(q *Select) error

func SelectLimit(limit int) SelectOption {
	return func(q *Select) error {
		if limit < 0 {
			return fmt.Errorf("limit: %w: %d", ErrLimit, limit)
		}
		q.limit = limit
		return nil
	}
}

func SelectOffset(offset int) SelectOption {
	return func(q *Select) error {
		if offset < 0 {
			return fmt.Errorf("offset: %w: %d", ErrLimit, offset)
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

func SelectGroupBy(columns ...SQLer) SelectOption {
	return func(q *Select) error {
		q.groupby = append(q.groupby, columns...)
		return nil
	}
}

func SelectHaving(having SQLer) SelectOption {
	return func(q *Select) error {
		if having == nil {
			return nil
		}
		if !acceptRelational(having) {
			return fmt.Errorf("having: %w", ErrSyntax)
		}
		q.having = having
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

func SelectDistinct() SelectOption {
	return func(q *Select) error {
		q.distinct = true
		return nil
	}
}

func SelectWith(name string, query Select, columns ...SQLer) SelectOption {
	return func(q *Select) error {
		if !isValidIdentifier(name) {
			return fmt.Errorf("with: %w %q", ErrIdent, name)
		}
		c := cte{
			name:    name,
			inner:   query,
			columns: append([]SQLer{}, columns...),
		}
		q.ctes = append(q.ctes, c)
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
	innerLeft:  "INNER JOIN",
	innerRight: "RIGHT INNER JOIN",
	outerLeft:  "LEFT OUTER JOIN",
	outerRight: "RIGHT OUTER JOIN",
}

type cte struct {
	name    string
	inner   SQLer
	columns []SQLer
}

func (c cte) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString(c.name)
	b.WriteString("(")
	as, err := writeSQL(&b, c.columns...)
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(")")
	b.WriteString(" AS ")
	b.WriteString("(")
	sql, as, err := c.inner.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(sql)
	b.WriteString(")")

	return b.String(), args, nil
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

func Using(list ...SQLer) SQLer {
	return NewList(list...)
}

type Select struct {
	ctes     []SQLer
	queries  []query
	where    SQLer
	orderby  []SQLer
	groupby  []SQLer
	having   SQLer
	limit    int
	offset   int
	distinct bool
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

func NewDistinct(table string, options ...SelectOption) (Select, error) {
	options = append(options, SelectDistinct())
	return NewSelect(table, options...)
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
	if !isJoinable(source) {
		return s, fmt.Errorf("%w: source can not be joined!", ErrSyntax)
	}
	switch cdt.(type) {
	case compare, and, or, list:
	default:
		return s, fmt.Errorf("%w: invalid condition type", ErrSyntax)
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
		ctes:    append([]SQLer{}, s.ctes...),
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

func (s Select) Exists() SQLer {
	return Exists(s)
}

func (s Select) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	if len(s.ctes) > 0 {
		b.WriteString("WITH ")
		as, err := writeSQL(&b, s.ctes...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(" ")
	}
	b.WriteString("SELECT ")
	if s.distinct {
		b.WriteString("DISTINCT ")
	}
	for i, q := range s.queries {
		if i > 0 {
			b.WriteString(", ")
		}
		if len(q.columns) == 0 {
			b.WriteString("*")
			continue
		}
		as, err := writeSQL(&b, q.columns...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
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

		if q.join != none {
			sql, as, err = q.cdt.SQL()
			if err != nil {
				return "", nil, err
			}
			args = append(args, as...)
			switch q.cdt.(type) {
			case and, or, compare:
				b.WriteString(" ON ")
				b.WriteString(sql)
			case list:
				b.WriteString(" USING (")
				b.WriteString(sql)
				b.WriteString(")")
			default:
				return "", nil, fmt.Errorf("join: %w", ErrSyntax)
			}
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
	if len(s.groupby) > 0 {
		b.WriteString(" GROUP BY ")
		as, err := writeSQL(&b, s.groupby...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		if s.having != nil {
			b.WriteString(" HAVING ")
			sql, as, err := s.having.SQL()
			if err != nil {
				return "", nil, err
			}
			args = append(args, as...)
			b.WriteString(sql)
		}
	}
	if len(s.orderby) > 0 {
		b.WriteString(" ORDER BY ")
		as, err := writeSQL(&b, s.orderby...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
	}
	if s.limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.Itoa(s.limit))
	}
	if s.offset > 0 {
		b.WriteString(" OFFSET ")
		b.WriteString(strconv.Itoa(s.offset))
	}
	return b.String(), args, nil
}

func (s Select) columnsCount() int {
	var c int
	for _, q := range s.queries {
		c += len(q.columns)
	}
	return c
}

type union struct {
	left  SQLer
	right SQLer
	all   bool
}

func Union(left, right SQLer) (SQLer, error) {
	return newUnion(left, right, false)
}

func UnionAll(left, right SQLer) (SQLer, error) {
	return newUnion(left, right, true)
}

func newUnion(left, right SQLer, all bool) (SQLer, error) {
	fst, ok := left.(Select)
	if !ok {
		return nil, ErrSyntax
	}
	snd, ok := right.(Select)
	if !ok {
		return nil, ErrSyntax
	}
	if fst.columnsCount() != snd.columnsCount() {
		return nil, fmt.Errorf("%w(union): columns count mismatch", ErrSyntax)
	}
	u := union{
		left:  left,
		right: right,
		all:   all,
	}
	return u, nil
}

func (u union) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	left, as, err := u.left.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(left)

	b.WriteString(" UNION ")
	if u.all {
		b.WriteString("ALL ")
	}

	right, as, err := u.right.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(right)

	return b.String(), args, nil
}

type exist struct {
	inner SQLer
}

func Exists(s SQLer) SQLer {
	return exist{inner: s}
}

func (e exist) SQL() (string, []interface{}, error) {
	var str string
	sql, args, err := e.inner.SQL()
	if err == nil {
		str = fmt.Sprintf("EXISTS %s", sql)
	}
	return str, args, err
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
