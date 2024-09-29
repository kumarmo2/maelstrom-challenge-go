package lib

import "fmt"

type AVLKey interface {
	Key() int
}

type AVLNode[T AVLKey] struct {
	item   T
	height int
	left   *AVLNode[T]
	right  *AVLNode[T]
}

func newAVLNode[T AVLKey](item T) *AVLNode[T] {
	return &AVLNode[T]{item: item, height: 0}
}

type AVLTree[T AVLKey] struct {
	root *AVLNode[T]
}

func NewAVLTRee[T AVLKey]() *AVLTree[T] {
	return &AVLTree[T]{}
}

func (self *AVLTree[T]) InsertItem(item T) {
	self.root = self.insertItemBinarySearch(self.root, item)
}

func height[T AVLKey](node *AVLNode[T]) int {
	if node == nil {
		return 0
	}
	return node.height
}

func (self *AVLTree[T]) insertItemBinarySearch(node *AVLNode[T], item T) *AVLNode[T] {
	if node == nil {
		node = newAVLNode(item)
	} else if item.Key() <= node.item.Key() {
		node.left = self.insertItemBinarySearch(node.left, item)
		if height(node.left)-height(node.right) == 2 {
			if item.Key() <= node.left.item.Key() {
				node = singleLLRotation(node)
			} else {
				node = doubleLRRotation(node)
			}
		}
	} else {
		node.right = self.insertItemBinarySearch(node.right, item)
		if height(node.right)-height(node.left) == 2 {
			if item.Key() >= node.right.item.Key() {
				node = singleRRRotation(node)
			} else {
				node = doubleRLRotation(node)
			}
		}
	}
	node.height = max(height(node.left), height(node.right)) + 1
	return node
}

func singleRRRotation[T AVLKey](node *AVLNode[T]) *AVLNode[T] {
	toReturn := node.right

	node.right = toReturn.left
	toReturn.left = node
	node.height = max(height(node.left), height(node.right)) + 1
	toReturn.height = max(height(toReturn.right), node.height) + 1

	return toReturn

}

func doubleLRRotation[T AVLKey](node *AVLNode[T]) *AVLNode[T] {
	node.left = singleRRRotation(node.left)
	return singleLLRotation(node)
}
func doubleRLRotation[T AVLKey](node *AVLNode[T]) *AVLNode[T] {
	node.right = singleLLRotation(node.right)
	return singleRRRotation(node)
}

func singleLLRotation[T AVLKey](node *AVLNode[T]) *AVLNode[T] {
	toReturn := node.left

	// BucketTreeNode toReturn = node.Left;
	node.left = toReturn.right
	toReturn.right = node
	node.height = max(height(node.left), height(node.right)) + 1
	toReturn.height = max(height(toReturn.left), node.height) + 1

	return toReturn
}

func (self *AVLTree[T]) PrintInorder() {
	self.printInorderBinarySearch(self.root)
}

func (tree *AVLTree[T]) ContainsKey(key int) bool {
	return tree.ContainsKeyRecursive(tree.root, key)
}

func (tree *AVLTree[T]) ContainsKeyRecursive(node *AVLNode[T], key int) bool {
	if node == nil {
		return false
	}
	if key < node.item.Key() {
		return tree.ContainsKeyRecursive(node.left, key)
	} else if key > node.item.Key() {
		return tree.ContainsKeyRecursive(node.right, key)
	}
	return true
}

func (self *AVLTree[T]) printInorderBinarySearch(node *AVLNode[T]) {
	if node == nil {
		return
	}
	self.printInorderBinarySearch(node.left)
	fmt.Println(node.item.Key())
	self.printInorderBinarySearch(node.right)
}

func (tree *AVLTree[T]) Height() int {
	return height(tree.root)
}

func (tree *AVLTree[T]) ToKeySlice() []int {
	slice := make([]int, 0)

	slice = tree.toKeySliceRecursive(tree.root, slice)

	return slice
}

func (tree *AVLTree[T]) toKeySliceRecursive(node *AVLNode[T], slice []int) []int {
	if node == nil {
		return slice
	}
	slice = tree.toKeySliceRecursive(node.left, slice)
	slice = append(slice, node.item.Key())
	slice = tree.toKeySliceRecursive(node.right, slice)
	return slice
}
