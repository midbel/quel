package quel

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
