package ast

import (
	"github.com/ichiban/btdb/store"
)

type TableDefinition struct {
	Name       string
	Columns    []ColumnDefinition
	PrimaryKey []string
	UniqueKeys [][]string
}

func (t *TableDefinition) SQL() string {
	return ""
}

func (t *TableDefinition) Execute(b *store.BTree) error {
	n, err := b.CreateRoot()
	if err != nil {
		return err
	}
	r, err := b.Insert(b.Root, store.Values{"table", t.Name}, store.Values{n, t.SQL()})
	if err != nil {
		return err
	}
	if b.Root == r {
		return nil
	}
	b.Root = r
	if err := b.UpdateHeader(); err != nil {
		return err
	}
	return nil
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
