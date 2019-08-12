package btdb

import (
	"context"
	"database/sql/driver"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/ichiban/btdb/sql"
	"github.com/ichiban/btdb/store"
)

type Database struct {
	tree *store.BTree
}

func Create(name string) (*Database, error) {
	t, err := store.Create(name, store.PageSize(4*1024), store.CellSize(512))
	if err != nil {
		return nil, err
	}
	r, err := t.CreateRoot()
	if err != nil {
		return nil, err
	}
	t.RootPageNo = store.PageNo(r)
	if err := t.UpdateHeader(); err != nil {
		return nil, err
	}
	return &Database{
		tree: t,
	}, nil
}

func Open(name string) (*Database, error) {
	t, err := store.Open(name)
	if err != nil {
		return nil, err
	}
	return &Database{
		tree: t,
	}, nil
}

func (d *Database) Close() error {
	return d.tree.Close()
}

func (d *Database) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	p := sql.NewParser(d.tree, query)
	s, err := p.DirectSQLStatement()
	if err != nil {
		return nil, err
	}
	sc, ok := s.(driver.StmtQueryContext)
	if !ok {
		return nil, xerrors.New("not implemented")
	}
	return sc.QueryContext(ctx, args)
}

func (d *Database) String() string {
	return fmt.Sprintf("%+v", d.tree)
}
