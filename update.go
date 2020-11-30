package quel

import (
	"fmt"
	"strings"
)

type UpdateOption func(*Update) error

func UpdateColumn(col SQLer) UpdateOption {
	return func(u *Update) error {
		if col != nil {
			u.columns = append(u.columns, col)
		}
		return nil
	}
}

func UpdateWhere(where SQLer) UpdateOption {
	return func(u *Update) error {
		if where == nil {
			return nil
		}
		if !acceptRelational(where) {
			return fmt.Errorf("where: %w", ErrSyntax)
		}
		u.where = where
		return nil
	}
}

func UpdateAlias(alias string) UpdateOption {
	return func(u *Update) error {
		if !isValidIdentifier(name) {
			return fmt.Errorf("alias: %w %q", ErrIdent, name)
		}
		u.table = Alias(alias, u.table)
		return nil
	}
}

type Update struct {
	table   SQLer
	columns []SQLer
	where   SQLer
}

func NewUpdate(table string, options ...UpdateOption) (Update, error) {
	var (
		u   Update
		err error
	)
	u.table = NewIdent(table)
	for _, opt := range options {
		if err = opt(&u); err != nil {
			break
		}
	}
	return u, err
}

func (u Update) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("UPDATE ")
	sql, as, err := u.table.SQL()
	if err != nil {
		return "", nil, err
	}
	b.WriteString(sql)
	b.WriteString(" SET ")
	for i, col := range u.columns {
		if i > 0 {
			b.WriteString(", ")
		}
		sql, as, err := col.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	if u.where != nil {
		b.WriteString(" WHERE ")
		sql, as, err := u.where.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	return b.String(), args, nil
}
