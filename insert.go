package quel

import (
	"fmt"
	"strings"
)

type InsertOption func(*Insert) error

func InsertColumns(columns ...string) InsertOption {
	return func(i *Insert) error {
		for _, c := range columns {
			if !isValidIdentifier(c) {
				return fmt.Errorf("column: %w %q", ErrIdent, c)
			}
			i.columns = append(i.columns, NewIdent(c))
		}
		return nil
	}
}

func InsertValues(values ...SQLer) InsertOption {
	return func(i *Insert) error {
		if len(values) == 0 {
			return fmt.Errorf("values: no values given")
		}
		i.values = append(i.values, values)
		return nil
	}
}

func InsertReturn(values ...SQLer) InsertOption {
	return func(i *Insert) error {
		i.returning = append(i.returning, values...)
		return nil
	}
}

type Insert struct {
	table     SQLer
	columns   []SQLer
	values    [][]SQLer
	returning []SQLer
}

func NewInsert(table string, options ...InsertOption) (Insert, error) {
	var (
		i   Insert
		err error
	)
	i.table = NewIdent(table)
	for _, opt := range options {
		if err = opt(&i); err != nil {
			break
		}
	}
	if len(i.values) == 0 {
		return i, fmt.Errorf("%w: no values given to be inserted", ErrSyntax)
	}
	return i, err
}

func (i Insert) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("INSERT INTO ")
	sql, _, err := i.table.SQL()
	if err != nil {
		return "", nil, err
	}
	b.WriteString(sql)
	if len(i.columns) > 0 {
		b.WriteString("(")
		as, err := writeSQL(&b, i.columns...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(")")
	}
	b.WriteString(" VALUES ")
	for j, vs := range i.values {
		if len(i.columns) > 0 && len(vs) != len(i.columns) {
			return "", nil, fmt.Errorf("insert: values mismatched number of columns")
		}
		if j > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		as, err := writeSQL(&b, vs...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(")")
	}
	if i.returning != nil {
		b.WriteString(" RETURNING ")
		as, err := writeSQL(&b, i.returning...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
	}
	return b.String(), args, nil
}
