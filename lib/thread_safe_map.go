package lib

import "sync"

type NicheThreadSafeMap[T comparable, V any] struct {
	// NOTE: This is a very niche Map that is only meant to be used for `GetOrCreateAndThenGet`.

	// NOTE: the reason for having 2 locks is to reduce the contention on the lock.
	// `valueGenerator` could take time to actually generate the value(eg: in the case of creation of a kafka log)
	// if we had a single lock and key doesn't exist, it would have acquired the lock till the valueGenerator returns, which
	// could take a long time. acquiring a lock for this long is not ideal.
	// therefore, we have 2 locks one for checking the "existence" of a key, and it is returned right after checking
	// if the key exists or not. and another lock for actually updating. with this, the operations when a key exists will be resolved
	// faster as the "existence" lock is freed quickly.

	inner *sync.Map
}

func NewNicheThreadSafeMap[T comparable, V any]() *NicheThreadSafeMap[T, V] {
	return &NicheThreadSafeMap[T, V]{inner: &sync.Map{}}
}

func (m *NicheThreadSafeMap[T, V]) GetOrCreateAndThenGet(key T, valueGenerator func() V) V {
	val, exists := m.inner.Load(key)
	if exists {
		return val.(V)
	}
	val = valueGenerator()
	m.inner.Store(key, val)
	return val.(V)
}

type GenericThreadSafeMap[K comparable, V any] struct {
	lock  *sync.RWMutex
	inner map[K]V
}

func NewGenericThreadSafeMap[K comparable, V any]() *GenericThreadSafeMap[K, V] {
	return &GenericThreadSafeMap[K, V]{
		lock:  &sync.RWMutex{},
		inner: map[K]V{},
	}
}

func (self *GenericThreadSafeMap[K, V]) Get(key K) (v V, exists bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	v, exists = self.inner[key]
	return v, exists
}

func (self *GenericThreadSafeMap[K, V]) Set(key K, val V) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.inner[key] = val
}

func (self *GenericThreadSafeMap[K, V]) ExposeInner() map[K]V {
	//NOTE: this must be used very carefully as we are exposing the inner map here.
	// if any concurrent actions are take on this inner map, its likely to panic.
	return self.inner
}
