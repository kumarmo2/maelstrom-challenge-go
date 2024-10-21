package lib

import "sync"

type ThreadSafeMap[T comparable, V any] struct {
	lock  *sync.RWMutex
	inner map[T]V
}

func NewThreadSafeMap[T comparable, V any]() *ThreadSafeMap[T, V] {
	return &ThreadSafeMap[T, V]{lock: &sync.RWMutex{}, inner: make(map[T]V)}
}

func (m *ThreadSafeMap[T, V]) GetOrCreateAndThenGet(key T, valueGenerator func() V) V {
	m.lock.Lock()
	defer m.lock.Unlock()
	val, exists := m.inner[key]
	if exists {
		return val
	}
	v := valueGenerator()
	m.inner[key] = v
	return v
}
