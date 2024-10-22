package lib

import "cmp"

type Key[K cmp.Ordered] interface {
	Key() K
}

type ANode[K cmp.Ordered, T Key[K]] struct {
	item   T
	height int
	left   *ANode[K, T]
	right  *ANode[K, T]
}

func newANode[K cmp.Ordered, T Key[K]](item T) *ANode[K, T] {
	return &ANode[K, T]{item: item, height: 0}
}

type AVLTree2[K cmp.Ordered, T Key[K]] struct {
	root *ANode[K, T]
}

func NewAVL2Tree[K cmp.Ordered, T Key[K]]() *AVLTree2[K, T] {
	return &AVLTree2[K, T]{}
}

func (self *AVLTree2[K, T]) InsertItem(item T) {
	self.root = self.insertItemBinarySearch(self.root, item)
}

func (self *AVLTree2[K, T]) Len() int {
	return self.lenRecursive(self.root)
}

func (self *AVLTree2[K, T]) lenRecursive(node *ANode[K, T]) int {
	if node == nil {
		return 0
	}
	return self.lenRecursive(node.left) + self.lenRecursive(node.right) + 1
}

func avlHeight[K cmp.Ordered, T Key[K]](node *ANode[K, T]) int {
	if node == nil {
		return 0
	}
	return node.height
}

// NOTE: we will only have unique "keys" in the tree.
func (self *AVLTree2[K, T]) insertItemBinarySearch(node *ANode[K, T], item T) *ANode[K, T] {
	if node == nil {
		node = newANode(item)
	} else if item.Key() == node.item.Key() {
		return node
	} else if item.Key() <= node.item.Key() {
		node.left = self.insertItemBinarySearch(node.left, item)
		if avlHeight(node.left)-avlHeight(node.right) == 2 {
			if item.Key() <= node.left.item.Key() {
				node = singleLLRotation2(node)
			} else {
				node = doubleLRRotation2(node)
			}
		}
	} else {
		node.right = self.insertItemBinarySearch(node.right, item)
		if avlHeight(node.right)-avlHeight(node.left) == 2 {
			if item.Key() >= node.right.item.Key() {
				node = singleRRRotation2(node)
			} else {
				node = doubleRLRotation2(node)
			}
		}
	}
	node.height = max(avlHeight(node.left), avlHeight(node.right)) + 1
	return node
}

func singleRRRotation2[K cmp.Ordered, T Key[K]](node *ANode[K, T]) *ANode[K, T] {
	toReturn := node.right

	node.right = toReturn.left
	toReturn.left = node
	node.height = max(avlHeight(node.left), avlHeight(node.right)) + 1
	toReturn.height = max(avlHeight(toReturn.right), node.height) + 1

	return toReturn

}

func doubleLRRotation2[K cmp.Ordered, T Key[K]](node *ANode[K, T]) *ANode[K, T] {
	node.left = singleRRRotation2(node.left)
	return singleLLRotation2(node)
}
func doubleRLRotation2[K cmp.Ordered, T Key[K]](node *ANode[K, T]) *ANode[K, T] {
	node.right = singleLLRotation2(node.right)
	return singleRRRotation2(node)
}

func singleLLRotation2[K cmp.Ordered, T Key[K]](node *ANode[K, T]) *ANode[K, T] {
	toReturn := node.left

	// BucketTreeNode toReturn = node.Left;
	node.left = toReturn.right
	toReturn.right = node
	node.height = max(avlHeight(node.left), avlHeight(node.right)) + 1
	toReturn.height = max(avlHeight(toReturn.left), node.height) + 1

	return toReturn
}

func (tree *AVLTree2[K, T]) ContainsKey(key K) bool {
	return tree.ContainsKeyRecursive(tree.root, key)
}

func (tree *AVLTree2[K, T]) ContainsKeyRecursive(node *ANode[K, T], key K) bool {
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

func (tree *AVLTree2[K, T]) Height() int {
	return avlHeight(tree.root)
}

func (tree *AVLTree2[K, T]) ToKeySlice() []K {
	slice := make([]K, 0)

	slice = tree.toKeySliceRecursive(tree.root, slice)

	return slice
}

func (tree *AVLTree2[K, T]) GetItemsGreaterThanInOrder(key K) []T {
	result := make([]T, 0)
	result = tree.getItemsGreaterThanInorder(tree.root, key, result)
	return result
}

// TODO: add unit test
func (tree *AVLTree2[K, T]) GetItemsGreaterThanAndIncludingInOrder(key K) []T {
	result := make([]T, 0)
	result = tree.getItemsGreaterAndIncludingThanInorder(tree.root, key, result)
	return result
}

func (tree *AVLTree2[K, T]) getItemsGreaterAndIncludingThanInorder(node *ANode[K, T], key K, slice []T) []T {
	if node == nil {
		return slice
	}
	// fmt.Printf("node.item.key: %v, key: %v\n", node.item.Key(), key)
	if node.item.Key() < key {
		slice = tree.getItemsGreaterAndIncludingThanInorder(node.right, key, slice)
	} else if node.item.Key() > key {
		slice = tree.getItemsGreaterAndIncludingThanInorder(node.left, key, slice)
		slice = append(slice, node.item)
		slice = tree.getItemsGreaterAndIncludingThanInorder(node.right, key, slice)
	} else {
		slice = append(slice, node.item)
		slice = tree.getItemsGreaterAndIncludingThanInorder(node.right, key, slice)
	}
	return slice

}

func (tree *AVLTree2[K, T]) getItemsGreaterThanInorder(node *ANode[K, T], key K, slice []T) []T {
	if node == nil {
		return slice
	}
	// fmt.Printf("node.item.key: %v, key: %v\n", node.item.Key(), key)
	if node.item.Key() <= key {
		slice = tree.getItemsGreaterThanInorder(node.right, key, slice)
	} else if node.item.Key() > key {
		slice = tree.getItemsGreaterThanInorder(node.left, key, slice)
		slice = append(slice, node.item)
		slice = tree.getItemsGreaterThanInorder(node.right, key, slice)
	}
	return slice

}

func (tree *AVLTree2[K, T]) toKeySliceRecursive(node *ANode[K, T], slice []K) []K {
	if node == nil {
		return slice
	}
	slice = tree.toKeySliceRecursive(node.left, slice)
	slice = append(slice, node.item.Key())
	slice = tree.toKeySliceRecursive(node.right, slice)
	return slice
}
