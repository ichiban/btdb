package sql

import (
	"database/sql/driver"

	"golang.org/x/xerrors"
)

type Rows struct {
	Err error

	cols []ColumnDefinition
	rows <-chan []interface{}
}

func (Rows) Columns() []string {
	panic("implement me")
}

func (Rows) Close() error {
	panic("implement me")
}

func (Rows) Next(dest []driver.Value) error {
	panic("implement me")
}

func (Rows) LastInsertId() (int64, error) {
	return 0, xerrors.New("not supported")
}

func (r Rows) RowsAffected() (int64, error) {
	c := int64(0)
	var vs []driver.Value
	for {
		if err := r.Next(vs); err != nil {
			return 0, err
		}
		c++
	}
	return c, nil
}
