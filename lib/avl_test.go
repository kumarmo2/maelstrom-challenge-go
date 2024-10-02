package lib

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type Int int

func (num Int) Key() int {
	return int(num)
}

func TestInorderPrint(t *testing.T) {
	tree := NewAVLTRee[Int]()
	tree.InsertItem(34)
	tree.InsertItem(100)
	tree.InsertItem(1)
	tree.InsertItem(50)
	tree.InsertItem(10)

	tree.PrintInorder()
	t.Log(tree.Height())

	result := tree.GetItemsGreaterThanInOrder(20)
	n := len(result)
	if n != 3 {
		panic("length should have been 3")
	}
	if result[0] != 34 {
		panic(fmt.Sprintf("expected 34, but got %v\n", result[0]))
	}
	if result[2] != 100 {
		panic(fmt.Sprintf("expected 100, but got %v\n", result[0]))

	}
	t.Log(len(result))

}

type Time struct {
	time time.Time
}

func (self *Time) Key() int {
	return int(self.time.UnixMilli())
}

func newTime() *Time {
	return &Time{time: time.Now()}

}
func TestInorderPrintWithTime(t *testing.T) {

	tree := NewAVLTRee[*Time]()
	tree.InsertItem(newTime())
	time.Sleep(time.Millisecond * 100)
	tree.InsertItem(newTime())
	time.Sleep(time.Millisecond * 100)
	tree.InsertItem(newTime())
	time.Sleep(time.Millisecond * 100)
	tree.InsertItem(newTime())
	ti := time.Now()
	time.Sleep(time.Millisecond * 100)
	tree.InsertItem(newTime())

	tree.PrintInorder()
	t.Log(tree.Height())

	result := tree.GetItemsGreaterThanInOrder(int(ti.UnixMilli()))
	t.Log(len(result))

}

func TestJsonSerde(t *testing.T) {
	slice := make([]*MessageItem, 0)
	msg := newMessageItem(12)
	slice = append(slice, msg)

	s, err := json.Marshal(slice)
	if err != nil {
		panic(err)
	}
	t.Logf("marshalled string length: %v", len(s))
	t.Logf("marshalled string : %v", string(s))

	s2 := make([]*MessageItem, 0)
	err = json.Unmarshal(s, &s2)
	if err != nil {
		panic(err)
	}
	t.Logf("unmarshalled value: %v", *s2[0])
}
