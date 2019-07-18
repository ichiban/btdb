package sql

import (
	"github.com/ichiban/btdb/store"
)

type InsertStatement struct {
	RawSQL string
	Target string
	Source Source
}

func (i *InsertStatement) SQL() string {
	return i.RawSQL
}

func (i *InsertStatement) Execute(b *store.BTree) error {
	vs, err := b.Search(b.Root, store.Values{"table", i.Target})
	if err != nil {
		return err
	}
	p := NewParser(vs[1].(string))
	_, err = p.TableDefinition()
	if err != nil {
		return err
	}
	return nil
}

type Source interface {
	Next() store.Values
}

type FromConstructorSource []store.Values

func (s *FromConstructorSource) Next() store.Values {
	if len(*s) == 0 {
		return nil
	}
	v := (*s)[0]
	*s = (*s)[1:]
	return v
}
