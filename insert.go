package quel

import (
	"strings"
)

type InsertOption func(*Insert) error

func InsertColumns(columns ...string) InsertOption {
	return func(i *Insert) error {
		for _, c := range columns {
			i.columns = append(i.columns, NewIdent(c))
		}
		return nil
	}
}

func InsertValues(values ...SQLer) InsertOption {
	return func(i *Insert) error {
		i.values = append(i.values, values)
		return nil
	}
}

type Insert struct {
	table   SQLer
	columns []SQLer
	values  [][]SQLer
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
	return i, err
}

func (i Insert) SQL() (string, []interface{}, error) {
	if len(i.values) == 0 {
		return "", nil, fmt.Errorf("insert: no values to be inserted")
	}
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("INSERT INTO ")
	b.WriteString(i.table)
	if len(i.columns) > 0 {
		b.WriteString("(")
		for j := range i.columns {
			if j > 0 {
				b.WriteString(", ")
			}
			sql, as, err := i.columns[j].SQL()
			if err != nil {
				return "", nil, err
			}
			b.WriteString(sql)
			args = append(args, as...)
		}
		b.WriteString(")")
	}
	b.WriteString(" VALUES ")
	for j, vs := range i.values {
		if len(vs) != len(i.columns) {
			return "", nil, fmt.Errorf("insert: values mismatched number of columns")
		}
		if j > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range vs {
			if j > 0 {
				b.WriteString(", ")
			}
			sql, as, err := vs[j].SQL()
			if err != nil {
				return "", nil, err
			}
			b.WriteString(sql)
			args = append(args, as...)
		}
		b.WriteString(")")
	}
	return b.String(), args, nil
}
