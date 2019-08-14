package sql

import (
	"database/sql/driver"
	"io"

	"golang.org/x/xerrors"
)

type Rows struct {
	Err error

	cols []string
	rows <-chan []driver.Value
}

func (r *Rows) Columns() []string {
	return r.cols
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

func (r *Rows) projection(cols []string) *Rows {
	mapping := make([]int, len(r.cols))

src:
	for i, sc := range r.cols {
		for j, dc := range cols {
			if sc == dc {
				mapping[i] = j
				continue src
			}
		}
		mapping[i] = -1
	}

	ch := make(chan []driver.Value)
	go func() {
		for r := range r.rows {
			dest := make([]driver.Value, len(r))
			for i, v := range r {
				m := mapping[i]
				if m >= 0 {
					dest[m] = v
				}
			}
			ch <- dest
		}
		close(ch)
	}()

	return &Rows{
		cols: cols,
		rows: ch,
	}
}
