package store

type Iterator struct {
	*cell

	btree *BTree
	page  *Page
	index int
}

func (i *Iterator) Next() error {
	if i.index == len(i.page.cells)-1 {
		if i.page.next == 0 {
			return ErrNotFound
		}
		p, err := i.btree.get(i.page.next)
		if err != nil {
			return err
		}
		i.page = p
		i.index = 0
	} else {
		i.index++
	}
	i.cell = &i.page.cells[i.index]
	return nil
}
