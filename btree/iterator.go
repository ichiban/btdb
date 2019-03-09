package btree

type Iterator struct {
	*Cell

	btree *BTree
	page  *Page
	index int
	err   error
}

func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	if i.index == len(i.page.Cells)-1 {
		if i.page.Next == 0 {
			return false
		}
		i.page, i.err = i.btree.get(i.page.Next)
		if i.err != nil {
			return false
		}
		i.index = 0
	} else {
		i.index++
	}
	i.Cell = &i.page.Cells[i.index]
	return true
}

func (i *Iterator) Err() error {
	return i.err
}
