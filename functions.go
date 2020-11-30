package quel

import (
	"strings"
)

type function struct {
	name string
	args []SQLer
}

func Func(name string, args ...SQLer) SQLer {
	return function{
		name: name,
		args: append([]SQLer{}, args...),
	}
}

func (f function) SQL() (string, []interface{}, error) {
	var b strings.Builder
	b.WriteString(f.name)
	b.WriteString("(")
	var args []interface{}
	for i := range f.args {
		if i > 0 {
			b.WriteString(", ")
		}
		sql, as, err := f.args[i].SQL()
		if err != nil {
			return "", nil, err
		}
		b.WriteString(sql)
		args = append(args, as...)
	}
	b.WriteString(")")
	return b.String(), args, nil
}
