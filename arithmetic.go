package quel

import (
	"fmt"
)

const (
	micros uint8 = iota
	millis
	second
	minute
	hour
	day
	week
	year
)

var durations = map[uint8]string{
	micros: "",
	millis: "",
	second: "",
	minute: "",
	hour:   "",
	day:    "",
	week:   "",
	year:   "",
}

type duration struct {
	unit uint8
	wait int
}

func Seconds(d int) SQLer {
	return newDuration(d, second)
}

func Minutes(d int) SQLer {
	return newDuration(d, minute)
}

func Hours(d int) SQLer {
	return newDuration(d, hour)
}

func Days(d int) SQLer {
	return newDuration(d, day)
}

func Weeks(d int) SQLer {
	return newDuration(d, week)
}

func Years(d int) SQLer {
	return newDuration(d, year)
}

func newDuration(d int, unit uint8) SQLer {
	return duration{
		unit: unit,
		wait: d,
	}
}

func (d duration) SQL() (string, []interface{}, error) {
	unit, ok := durations[d.unit]
	if !ok {
		return "", nil, fmt.Errorf("unsupported duration")
	}
	return fmt.Sprintf("INTERVAL %d %s", d.wait, unit), nil, nil
}

const (
	add uint8 = iota
	sub
	mul
	div
	mod
	bitor
	bitand
	bitnot
)

var mathops = map[uint8]string{
	add:    "+",
	sub:    "-",
	mul:    "*",
	div:    "/",
	mod:    "%",
	bitand: "&",
	bitor:  "|",
	bitnot: "^",
}

type arithmetic struct {
	left  SQLer
	right SQLer
	op    uint8
}

func Add(left, right SQLer) SQLer {
	return arithmetic{
		left:  left,
		right: right,
		op:    add,
	}
}

func Subtract(left, right SQLer) SQLer {
	return arithmetic{
		left:  left,
		right: right,
		op:    sub,
	}
}

func Divide(left, right SQLer) SQLer {
	return arithmetic{
		left:  left,
		right: right,
		op:    div,
	}
}

func Multiply(left, right SQLer) SQLer {
	return arithmetic{
		left:  left,
		right: right,
		op:    mul,
	}
}

func Modulo(left, right SQLer) SQLer {
	return arithmetic{
		left:  left,
		right: right,
		op:    mod,
	}
}

func (a arithmetic) SQL() (string, []interface{}, error) {
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

	op, ok := mathops[a.op]
	if !ok {
		return "", nil, fmt.Errorf("unsupported arithmetic operator")
	}
	if _, ok := a.left.(arithmetic); ok {
		left = fmt.Sprintf("(%s)", left)
	}
	if _, ok := a.right.(arithmetic); ok {
		right = fmt.Sprintf("(%s)", right)
	}
	return fmt.Sprintf("%s %s %s", left, op, right), args, nil
}
