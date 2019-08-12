package sql

import (
	"context"
	"database/sql/driver"
)

type TableDefinition struct {
	store Store

	RawSQL     string
	Name       string
	Columns    []ColumnDefinition
	PrimaryKey []string
	UniqueKeys [][]string
}

func (t *TableDefinition) Close() error {
	return nil
}

func (t *TableDefinition) NumInput() int {
	return 0
}

func (t *TableDefinition) Exec(args []driver.Value) (driver.Result, error) {
	return t.ExecContext(context.Background(), namedValues(args))
}

func (t *TableDefinition) Query(args []driver.Value) (driver.Rows, error) {
	return t.QueryContext(context.Background(), namedValues(args))
}

func (t *TableDefinition) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	r, err := t.QueryContext(ctx, args)
	return r.(driver.Result), err
}

func (t *TableDefinition) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	n, err := t.store.CreateRoot()
	if err != nil {
		return nil, err
	}
	r, err := t.store.Insert(t.store.Root(), []interface{}{"table", t.Name}, []interface{}{n, t.RawSQL})
	if err != nil {
		return nil, err
	}
	if t.store.Root() == r {
		return nil, nil
	}
	if err := t.store.UpdateRoot(r); err != nil {
		return nil, err
	}
	return nil, nil
}

func (t *TableDefinition) primaryKey(c string) bool {
	for _, p := range t.PrimaryKey {
		if p == c {
			return true
		}
	}
	return false
}

type ColumnDefinition struct {
	Name     string
	DataType DataType
}

type DataType int

const (
	Text DataType = iota
	Integer
)
