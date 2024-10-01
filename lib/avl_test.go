package lib

import "testing"

type Int int

func (num Int) Key() int {
	return int(num)
}

func TestInorderPrint(t *testing.T) {
	tree := NewAVLTRee[Int]()
	tree.InsertItem(1)
	tree.InsertItem(2)
	tree.InsertItem(3)
	tree.InsertItem(4)

	tree.PrintInorder()
	t.Log(tree.Height())

	result := tree.GetItemsGreaterThan(1)
	t.Log(len(result))

}
