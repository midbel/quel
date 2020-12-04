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

func (f function) Alias(name string) SQLer {
	return Alias(name, f)
}

func (f function) SQL() (string, []interface{}, error) {
	var (
		b    strings.Builder
		args []interface{}
	)
	b.WriteString(f.name)
	b.WriteString("(")
	as, err := writeSQL(&b, f.args...)
	if err != nil {
		return "", nil, err
	}
	args = append(args, as...)
	b.WriteString(")")
	return b.String(), args, nil
}

func Count(ident SQLer) SQLer {
	return Func("COUNT", ident)
}

func Sum(ident SQLer) SQLer {
	return Func("SUM", ident)
}

func Coalesce(values ...SQLer) SQLer {
	return Func("COALESCE", values...)
}

func Now() SQLer {
	return Func("NOW")
}

func If(csq, alt SQLer) SQLer {
	return Func("IF", csq, alt)
}

func Min(column SQLer) SQLer {
	return Func("MIN", column)
}

func Max(column SQLer) SQLer {
	return Func("MAX", column)
}

func IsNull(expr SQLer) SQLer {
	return Func("ISNULL", expr)
}
func Date(expr SQLer) SQLer {
	return Func("DATE", expr)
}
