package sql

type Store interface {
	Root() int
	CreateRoot() (int, error)
	UpdateRoot(root int) error
	Insert(root int, key, value []interface{}) (int, error)
	Search(root int, key []interface{}) ([]interface{}, error)
	Update(root int, key, val []interface{}) error
}
