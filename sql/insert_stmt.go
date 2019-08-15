package sql

import (
	"context"
	"database/sql/driver"

	"github.com/ichiban/btdb/store"
)

type InsertStatement struct {
	store  *store.BTree
	Target string
	Source *Rows
}

func (i *InsertStatement) Close() error {
	return nil
}

func (i *InsertStatement) NumInput() int {
	return 0
}

func (i *InsertStatement) Exec(args []driver.Value) (driver.Result, error) {
	return i.ExecContext(context.Background(), namedValues(args))
}

func (i *InsertStatement) Query(args []driver.Value) (driver.Rows, error) {
	return i.QueryContext(context.Background(), namedValues(args))
}

func (i *InsertStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	r, err := i.QueryContext(ctx, args)
	return r.(driver.Result), err
}

func (i *InsertStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	tk := []interface{}{"table", i.Target}

	vs, err := i.store.Search(i.store.Root(), tk)
	if err != nil {
		return nil, err
	}

	p := NewParser(i.store, vs[1].(string))
	td, err := p.TableDefinition()
	if err != nil {
		return nil, err
	}

	cols := td.columnNames()

	ch := make(chan []driver.Value)
	rows := Rows{
		cols: cols,
		rows: ch,
	}

	go func() {
		src := i.Source.projection(cols)

		for {
			val := make([]driver.Value, len(td.Columns))
			if err := src.Next(val); err != nil {
				rows.Err = err
				break
			}
			k := make([]interface{}, 0, len(td.PrimaryKey))
			v := make([]interface{}, 0, len(td.Columns)-len(td.PrimaryKey))

			for i, c := range td.Columns {
				if td.primaryKey(c.Name) {
					k = append(k, val[i])
				} else {
					v = append(v, val[i])
				}
			}

			or := vs[0].(uint64)
			nr, err := i.store.Insert(int(or), k, v)
			if err != nil {
				rows.Err = err
				break
			}

			ch <- val

			if nr != int(or) {
				vs[0] = nr
				if err := i.store.Update(i.store.Root(), tk, vs); err != nil {
					rows.Err = err
					break
				}
			}
		}
		close(ch)
	}()

	return &rows, nil
}
