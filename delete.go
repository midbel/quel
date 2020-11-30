package quel

type DeleteOption func(*Delete) error

type Delete struct {
	table SQLer
	where SQLer
}

func NewDelete(table string, options ...DeleteOption) (Delete, error) {
	var d Delete
	return d, nil
}
