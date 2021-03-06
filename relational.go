package quel

import (
	"fmt"
	"strings"
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
	isnull
	isnotnull
)

var cmpops = map[uint8]string{
	equal:     "=",
	noteq:     "<>",
	less:      "<",
	lesseq:    "<=",
	great:     ">",
	greateq:   ">=",
	like:      "LIKE",
	notlike:   "NOT LIKE",
	in:        "IN",
	notin:     "NOT IN",
	isnull:    "IS NULL",
	isnotnull: "IS NOT NULL",
}

type compare struct {
	left  SQLer
	right SQLer
	op    uint8
}

func Equal(left, right SQLer) SQLer {
	return newCompare(equal, left, right)
}

func NotEqual(left, right SQLer) SQLer {
	return newCompare(noteq, left, right)
}

func LesserThan(left, right SQLer) SQLer {
	return newCompare(less, left, right)
}

func LesserOrEqual(left, right SQLer) SQLer {
	return newCompare(lesseq, left, right)
}

func GreaterThan(left, right SQLer) SQLer {
	return newCompare(great, left, right)
}

func GreaterOrEqual(left, right SQLer) SQLer {
	return newCompare(greateq, left, right)
}

func Like(left, right SQLer) SQLer {
	return newCompare(like, left, right)
}

func NotLike(left, right SQLer) SQLer {
	return newCompare(notlike, left, right)
}

func In(left, right SQLer) SQLer {
	return newCompare(in, left, right)
}

func NotIn(left, right SQLer) SQLer {
	return newCompare(notin, left, right)
}

func IsNullTest(left SQLer) SQLer {
	return newCompare(isnull, left, nil)
}

func IsNotNullTest(left SQLer) SQLer {
	return newCompare(isnotnull, left, nil)
}

func newCompare(op uint8, left, right SQLer) SQLer {
	return compare{
		left:  left,
		right: right,
		op:    op,
	}
}

func (c compare) SQL() (string, []interface{}, error) {
	var args []interface{}
	left, as, err := c.left.SQL()
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)

	if c.op == isnull || c.op == isnotnull {
		return fmt.Sprintf("%s %s", left, cmpops[c.op]), args, nil
	}

	right, as, err := c.right.SQL()
	if err != nil {
		return "", nil, err
	}

	switch c.right.(type) {
	case list, Select:
		right = fmt.Sprintf("(%s)", right)
	default:
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

type CaseOption func(k *kase) error

func CaseAlternative(alt SQLer) CaseOption {
	return func(k *kase) error {
		if alt != nil {
			k.alt = alt
		}
		return nil
	}
}

func CaseWhen(test, csq SQLer) CaseOption {
	return func(k *kase) error {
		if test != nil && csq != nil {
			k.test = append(k.test, test)
			k.csq = append(k.csq, csq)
		}
		return nil
	}
}

func CaseExpr(expr SQLer) CaseOption {
	return func(k *kase) error {
		if expr != nil {
			k.expr = expr
		}
		return nil
	}
}

type kase struct {
	expr SQLer
	test []SQLer
	csq  []SQLer
	alt  SQLer
}

func NewCase() SQLer {
	return nil
}

func (k kase) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString("CASE ")
	if k.expr != nil {
		sql, as, err := k.expr.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
		b.WriteString(" ")
	}
	for i := range k.test {
		b.WriteString("WHEN ")
		sql, as, err := k.test[i].SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
		b.WriteString(" THEN ")
		sql, as, err = k.csq[i].SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	if k.alt != nil {
		b.WriteString(" ELSE ")
		sql, as, err := k.alt.SQL()
		if err != nil {
			return "", nil, err
		}
		args = append(args, as...)
		b.WriteString(sql)
	}
	b.WriteString(" END")
	return b.String(), args, nil
}

func (k kase) Alias(name string) SQLer {
	return Alias(name, k)
}

type any struct {
	inner SQLer
}

func (a any) SQL() (string, []interface{}, error) {
	sql, args, err := a.inner.SQL()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("ANY (%s)", sql), args, nil
}

type all struct {
	inner SQLer
}

func (a all) SQL() (string, []interface{}, error) {
	sql, args, err := a.inner.SQL()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("ALL (%s)", sql), args, nil
}

func acceptRelational(part SQLer) bool {
	switch part.(type) {
	case compare, and, or:
		return true
	default:
		return false
	}
}
