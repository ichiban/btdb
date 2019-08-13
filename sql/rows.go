package sql

import (
	"database/sql/driver"
	"io"

	"golang.org/x/xerrors"
)

type Rows struct {
	Err error

	cols []ColumnDefinition
	rows <-chan []interface{}
}

func (r *Rows) Columns() []string {
	cs := make([]string, len(r.cols))
	for i, col := range r.cols {
		cs[i] = col.Name
	}
	return cs
}

func (Rows) Close() error {
	panic("implement me")
}

func (r *Rows) Next(dest []driver.Value) error {
	if err := r.Err; err != nil {
		return err
	}
	row, ok := <-r.rows
	if !ok {
		return io.EOF
	}
	for i, v := range row {
		dest[i] = v
	}
	return nil
}

func (Rows) LastInsertId() (int64, error) {
	return 0, xerrors.New("not supported")
}

func (r *Rows) RowsAffected() (int64, error) {
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
