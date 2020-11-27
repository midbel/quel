package quel

import (
  "errors"
)

var (
	ErrIdent  = errors.New("invalid identifier")
	ErrLimit  = errors.New("negative limit")
	ErrSyntax = errors.New("invalid syntax")
)

const null = "null"

type SQLer interface {
	SQL() (string, []interface{}, error)
}

type SqlMarshaler interface {
	MarshalSQL() string
}
