package sql

import "github.com/ichiban/btdb/store"

type Statement interface {
	SQL() string
	Execute(tree *store.BTree) error
}
