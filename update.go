package quel

import (
	"fmt"
	"strings"
)

type UpdateOption func(*Update) error

func UpdateColumn(ident string, value SQLer) UpdateOption {
	return func(u *Update) error {
		if !isValidIdentifier(ident) {
			return fmt.Errorf("update: %w %q", ErrIdent, ident)
		}
		u.columns = append(u.columns, Equal(NewIdent(ident), value))
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
		if !isValidIdentifier(alias) {
			return fmt.Errorf("alias: %w %q", ErrIdent, alias)
		}
		u.table = Alias(alias, u.table)
		return nil
	}
}

func UpdateReturn(values ...SQLer) UpdateOption {
	return func(u *Update) error {
		u.returning = append(u.returning, values...)
		return nil
	}
}

type Update struct {
	table     SQLer
	columns   []SQLer
	where     SQLer
	returning []SQLer
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
	if len(u.columns) == 0 {
		return u, fmt.Errorf("%w: no columns to update", ErrSyntax)
	}
	return u, err
}

func (u Update) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("UPDATE ")
	sql, _, err := u.table.SQL()
	if err != nil {
		return "", nil, err
	}
	b.WriteString(sql)
	b.WriteString(" SET ")
	as, err := writeSQL(&b, u.columns...)
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	if u.where != nil {
		b.WriteString(" WHERE ")
		sql, as, err := u.where.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	if u.returning != nil {
		b.WriteString(" RETURNING ")
		as, err := writeSQL(&b, u.returning...)
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
	}
	return b.String(), args, nil
}
