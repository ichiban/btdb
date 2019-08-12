package sql

import "database/sql/driver"

type SelectStatement struct {
	From string
}

func (q *SelectStatement) Close() error {
	return nil
}

func (q *SelectStatement) NumInput() int {
	return 0
}

func (q *SelectStatement) Exec(args []driver.Value) (driver.Result, error) {
	panic("implement me")
}

func (q *SelectStatement) Query(args []driver.Value) (driver.Rows, error) {
	panic("implement me")
}
