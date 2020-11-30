package quel

import (
	"fmt"
)

const (
	equal uint8 = iota
	noteq
	less
	lesseq
	great
	greateq
	like
	notlike
	in
	notin
)

var cmpops = map[uint8]string{
	equal:   "=",
	noteq:   "<>",
	less:    "<",
	lesseq:  "<=",
	great:   ">",
	greateq: ">=",
	like:    "LIKE",
	notlike: "NOT LIKE",
	in:      "IN",
	notin:   "NOT IN",
}

type compare struct {
	left  SQLer
	right SQLer
	op    uint8
}

func Equal(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    equal,
	}
}

func NotEqual(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    noteq,
	}
}

func LesserThan(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    less,
	}
}

func LesserOrEqual(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    lesseq,
	}
}

func GreaterThan(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    great,
	}
}

func GreaterOrEqual(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    greateq,
	}
}

func Like(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    like,
	}
}

func NotLike(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    notlike,
	}
}

func In(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    in,
	}
}

func NotIn(left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    notin,
	}
}

func (c compare) SQL() (string, []interface{}, error) {
	var args []interface{}
	left, as, err := c.left.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	right, as, err := c.right.SQL()
	if err != nil {
		return "", nil, err
	}

	if c.op == in || c.op == notin {
		if _, ok := c.right.(list); !ok {
			return "", nil, ErrSyntax
		}
		right = fmt.Sprintf("(%s)", right)
	}

	args = append(args, as...)

	op, ok := cmpops[c.op]
	if !ok {
		return "", nil, fmt.Errorf("unsupported comparison operator")
	}
	return fmt.Sprintf("%s %s %s", left, op, right), args, nil
}

type between struct {
	value SQLer
	left  SQLer
	right SQLer
}

func Between(value, left, right SQLer) SQLer {
	return between{
		value: value,
		left:  left,
		right: right,
	}
}

func (b between) SQL() (string, []interface{}, error) {
	var (
		sql   string
		left  string
		right string
		args  []interface{}
		as    []interface{}
		err   error
	)
	if sql, as, err = b.value.SQL(); err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	if left, as, err = b.left.SQL(); err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	if right, as, err = b.right.SQL(); err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	return fmt.Sprintf("%s BETWEEN %s AND %s", sql, left, right), args, nil
}

type not struct {
	right SQLer
}

func Not(right SQLer) SQLer {
	return not{
		right: right,
	}
}

func (n not) SQL() (string, []interface{}, error) {
	if !acceptRelational(n.right) {
		return "", nil, fmt.Errorf("not: %w", ErrSyntax)
	}
	right, args, err := n.right.SQL()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("NOT %s", right), args, nil
}

type and struct {
	left  SQLer
	right SQLer
}

func And(left, right SQLer) SQLer {
	return and{
		left:  left,
		right: right,
	}
}

func (a and) SQL() (string, []interface{}, error) {
	if !acceptRelational(a.left) {
		return "", nil, fmt.Errorf("and(left): %w", ErrSyntax)
	}

	if !acceptRelational(a.right) {
		return "", nil, fmt.Errorf("and(right): %w", ErrSyntax)
	}

	var args []interface{}
	left, as, err := a.left.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	right, as, err := a.right.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	var b strings.Builder
	switch a.left.(type) {
	case and, or:
		b.WriteString("(")
		b.WriteString(left)
		b.WriteString(")")
	default:
		b.WriteString(left)
	}

	b.WriteString(" AND ")

	switch a.right.(type) {
	case and, or:
		b.WriteString("(")
		b.WriteString(right)
		b.WriteString(")")
	default:
		b.WriteString(right)
	}

	return b.String(), args, nil
}

type or struct {
	left  SQLer
	right SQLer
}

func Or(left, right SQLer) SQLer {
	return or{
		left:  left,
		right: right,
	}
}

func (o or) SQL() (string, []interface{}, error) {
	if !acceptRelational(o.left) {
		return "", nil, fmt.Errorf("or(left): %w", ErrSyntax)
	}

	if !acceptRelational(o.right) {
		return "", nil, fmt.Errorf("or(right): %w", ErrSyntax)
	}

	var args []interface{}
	left, as, err := o.left.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	right, as, err := o.right.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	var b strings.Builder
	switch o.left.(type) {
	case and, or:
		b.WriteString("(")
		b.WriteString(left)
		b.WriteString(")")
	default:
		b.WriteString(left)
	}

	b.WriteString(" AND ")

	switch o.right.(type) {
	case and, or:
		b.WriteString("(")
		b.WriteString(right)
		b.WriteString(")")
	default:
		b.WriteString(right)
	}

	return b.String(), args, nil
}

func acceptRelational(part SQLer) bool {
	switch part.(type) {
	case compare, and, or:
		return true
	default:
		return false
	}
}
