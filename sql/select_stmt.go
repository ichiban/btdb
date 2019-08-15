package sql

import (
	"context"
	"database/sql/driver"

	"golang.org/x/xerrors"

	"github.com/ichiban/btdb/store"
)

type SelectStatement struct {
	store *store.BTree

	From string
}

func (q *SelectStatement) Close() error {
	return nil
}

func (q *SelectStatement) NumInput() int {
	return 0
}

func (q *SelectStatement) Exec(args []driver.Value) (driver.Result, error) {
	return q.ExecContext(context.Background(), namedValues(args))
}

func (q *SelectStatement) Query(args []driver.Value) (driver.Rows, error) {
	return q.QueryContext(context.Background(), namedValues(args))
}

func (q *SelectStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	r, err := q.QueryContext(ctx, args)
	return r.(driver.Result), err
}

func (q *SelectStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	tk := []interface{}{"table", q.From}

	vs, err := q.store.Search(q.store.Root(), tk)
	if err != nil {
		return nil, xerrors.Errorf("failed to search: %w", err)
	}

	p := NewParser(q.store, vs[1].(string))
	td, err := p.TableDefinition()
	if err != nil {
		return nil, xerrors.Errorf("failed to parse: %w", err)
	}

	r := vs[0].(uint64)
	iter, err := q.store.First(int(r))
	if err != nil {
		return nil, xerrors.Errorf("failed to get first: %w", err)
	}

	ch := make(chan []driver.Value)

	pk := td.PrimaryKey
	npk := td.nonPrimaryKey()
	rows := Rows{
		cols: append(pk, npk...),
		rows: ch,
	}

	go func() {
		for {
			if err := iter.Next(); err != nil {
				if err == store.ErrNotFound {
					break
				}
				rows.Err = xerrors.Errorf("failed iterate: %w", err)
				break
			}
			vs := make([]driver.Value, 0, len(td.Columns))
			for _, v := range iter.Key {
				vs = append(vs, v)
			}
			for _, v := range iter.Value {
				vs = append(vs, v)
			}
			ch <- vs
		}
		close(ch)
	}()

	return rows.projection(td.columnNames()), nil
}
