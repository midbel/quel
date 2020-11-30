package quel

import (
	"fmt"
	"strings"
)

type DeleteOption func(*Delete) error

func DeleteWhere(where SQLer) DeleteOption {
	return func(d *Delete) error {
		if where == nil {
			return nil
		}
		if !acceptRelational(where) {
			return fmt.Errorf("where: %w", ErrSyntax)
		}
		d.where = where
		return nil
	}
}

func DeleteAlias(alias string) DeleteOption {
	return func(d *Delete) error {
		if !isValidIdentifier(alias) {
			return fmt.Errorf("alias: %w %q", ErrIdent, alias)
		}
		d.table = Alias(alias, d.table)
		return nil
	}
}

type Delete struct {
	table SQLer
	where SQLer
}

func NewDelete(table string, options ...DeleteOption) (Delete, error) {
	var (
		d   Delete
		err error
	)
	d.table = NewIdent(table)
	for _, opt := range options {
		if err = opt(&d); err != nil {
			break
		}
	}
	return d, err
}

func (d Delete) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("DELETE FROM ")
	sql, as, err := d.table.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(sql)
	if d.where != nil {
		b.WriteString(" WHERE ")
		sql, as, err := d.where.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	return b.String(), args, nil
}
