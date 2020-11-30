package quel

type UpdateOption func(*Update) error

type Update struct {
	table SQLer
	where SQLer
}

func NewUpdate(table string, options ...UpdateOption) (Update, error) {
	var u Update
	return u, nil
}
