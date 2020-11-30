package quel

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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

type ident struct {
	name    string
	parents []string
}

func NewIdent(name string, parents ...string) SQLer {
	return ident{
		name:    name,
		parents: append([]string{}, parents...),
	}
}

func (i ident) SQL() (string, []interface{}, error) {
	for j := range i.parents {
		if !isValidIdentifier(i.parents[j]) {
			return "", nil, fmt.Errorf("ident: %w %q", ErrIdent, i.parents[j])
		}
	}
	if !isValidIdentifier(i.name) {
		return "", nil, fmt.Errorf("ident: %w %q", ErrIdent, i.name)
	}
	lines := append([]string{}, i.parents...)
	return strings.Join(append(lines, i.name), "."), nil, nil
}

type alias struct {
	SQLer
	name string
}

func Alias(name string, sql SQLer) SQLer {
	if _, ok := sql.(alias); ok {
		return sql
	}
	return alias{
		SQLer: sql,
		name:  name,
	}
}

func (a alias) SQL() (string, []interface{}, error) {
	sql, args, err := a.SQLer.SQL()
	if err != nil {
		return "", nil, err
	}
	if _, ok := a.SQLer.(Select); ok {
		return fmt.Sprintf("(%s) AS %s", sql, a.name), args, nil
	}
	return fmt.Sprintf("%s AS %s", sql, a.name), args, nil
}

type list struct {
	parts []SQLer
}

func NewList(parts ...SQLer) SQLer {
	return list{
		parts: append([]SQLer{}, parts...),
	}
}

func (i list) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	for j, p := range i.parts {
		if j > 0 {
			b.WriteString(", ")
		}
		sql, as, err := p.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	return b.String(), args, nil
}

type literal struct {
	value interface{}
}

func NewLiteral(value interface{}) SQLer {
	return literal{
		value: value,
	}
}

func (i literal) SQL() (string, []interface{}, error) {
	var str string
	switch val := i.value.(type) {
	case int:
		str = strconv.FormatInt(int64(val), 10)
	case int8:
		str = strconv.FormatInt(int64(val), 10)
	case int16:
		str = strconv.FormatInt(int64(val), 10)
	case int32:
		str = strconv.FormatInt(int64(val), 10)
	case int64:
		str = strconv.FormatInt(val, 10)
	case uint:
		str = strconv.FormatUint(uint64(val), 10)
	case uint8:
		str = strconv.FormatUint(uint64(val), 10)
	case uint16:
		str = strconv.FormatUint(uint64(val), 10)
	case uint32:
		str = strconv.FormatUint(uint64(val), 10)
	case uint64:
		str = strconv.FormatUint(val, 10)
	case float32:
		str = strconv.FormatFloat(float64(val), 'g', -1, 64)
	case float64:
		str = strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		str = strconv.FormatBool(val)
	case string:
		str = fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case time.Time:
		str = val.Format(time.RFC3339)
	default:
		if s, ok := i.value.(SqlMarshaler); ok {
			str = s.MarshalSQL()
		} else if s, ok := i.value.(fmt.Stringer); ok {
			str = s.String()
			break
		}
		return str, nil, ErrSyntax
	}
	return str, nil, nil
}

type arg struct {
	name  string
	value interface{}
}

func Arg(name string, a interface{}) SQLer {
	return arg{
		name:  name,
		value: a,
	}
}

func (a arg) SQL() (string, []interface{}, error) {
	if a.value == nil {
		a.value = null
	}
	return fmt.Sprintf("@%s", a.name), []interface{}{a.value}, nil
}

func writeSQL(b io.StringWriter, parts ...SQLer) ([]interface{}, error) {
	var args []interface{}
	for i, s := range parts {
		if i > 0 {
			b.WriteString(", ")
		}
		sql, as, err := s.SQL()
		if err != nil {
			return nil, err
		}
		b.WriteString(sql)
		args = append(args, as...)
	}
	return args, nil
}

const (
	dquote     = '"'
	squote     = '\''
	bquote     = '`'
	star       = '*'
	underscore = '_'
	dot        = '.'
)

func isValidIdentifier(ident string) bool {
	switch c, z := utf8.DecodeRuneInString(ident); c {
	case utf8.RuneError:
		return false
	case star:
		return true
	case squote, dquote, bquote:
		for {
			k, x := utf8.DecodeRuneInString(ident[z:])
			z += x
			if isQuote(k) || k == utf8.RuneError {
				c = k
				break
			}
		}
		if !isQuote(c) {
			return false
		}
		c, _ = utf8.DecodeRuneInString(ident[z:])
		return c == utf8.RuneError
	default:
		if isKeyword(ident) {
			return false
		}
		if !isLetter(c) {
			return false
		}
		z = 0
		for {
			k, x := utf8.DecodeRuneInString(ident[z:])
			if k == utf8.RuneError && x == 0 {
				break
			}
			if !isIdent(k) && k != dot {
				return false
			}
			z += x
		}
		return true
	}
}

var keywords = []string{}

func isKeyword(str string) bool {
	if len(keywords) == 0 {
		return false
	}
	x := sort.SearchStrings(keywords, strings.ToUpper(str))
	return x < len(keywords) && keywords[x] == strings.ToUpper(str)
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isIdent(r rune) bool {
	return isDigit(r) || isLetter(r) || r == underscore
}

func isQuote(r rune) bool {
	return r == dquote || r == squote || r == bquote
}
