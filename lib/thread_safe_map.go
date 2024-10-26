package lib

import "sync"

type ThreadSafeMap[T comparable, V any] struct {
	// NOTE: the reason for having 2 locks is to reduce the contention on the lock.
	// `valueGenerator` could take time to actually generate the value(eg: in the case of creation of a kafka log)
	// if we had a single lock and key doesn't exist, it would have acquired the lock till the valueGenerator returns, which
	// could take a long time. acquiring a lock for this long is not ideal.
	// therefore, we have 2 locks one for checking the "existence" of a key, and it is returned right after checking
	// if the key exists or not. and another lock for actually updating. with this, the operations when a key exists will be resolved
	// faster as the "existence" lock is freed quickly.

	mutexForCheckingExistence *sync.RWMutex
	mutexForUpdating          *sync.RWMutex
	inner                     map[T]V
}

func NewThreadSafeMap[T comparable, V any]() *ThreadSafeMap[T, V] {
	return &ThreadSafeMap[T, V]{mutexForCheckingExistence: &sync.RWMutex{}, inner: make(map[T]V), mutexForUpdating: &sync.RWMutex{}}
}

func (m *ThreadSafeMap[T, V]) ExposeInner() map[T]V {
	return m.inner
}
func (m *ThreadSafeMap[T, V]) Set(key T, val V) {
	m.mutexForUpdating.Lock()
	defer m.mutexForUpdating.Unlock()
	m.inner[key] = val
}

func (m *ThreadSafeMap[T, V]) Get(key T) (V, bool) {
	m.mutexForUpdating.Lock()
	defer m.mutexForUpdating.Unlock()
	v, exists := m.inner[key]
	return v, exists
}

func (m *ThreadSafeMap[T, V]) ContainsKey(key T) bool {
	m.mutexForCheckingExistence.Lock()
	defer m.mutexForCheckingExistence.Unlock()
	_, exists := m.inner[key]
	return exists
}

func (m *ThreadSafeMap[T, V]) GetOrCreateAndThenGet(key T, valueGenerator func() V) V {
	m.mutexForCheckingExistence.Lock()
	val, exists := m.inner[key]
	if exists {
		m.mutexForCheckingExistence.Unlock()
		return val
	}
	m.mutexForCheckingExistence.Unlock()
	m.mutexForUpdating.Lock()
	defer m.mutexForUpdating.Unlock()

	//NOTE: checking again for the existence of the key is necessary, because of having the 2 locks, to prevent double writes.
	// The reason why we need 2 locks is mentioned near the definition  of the ThreadSafeMap.

	val, exists = m.inner[key]
	if exists {
		return val
	}
	val = valueGenerator() // TODO: if valueGenerator paniced, we should make sure the mutexes are not poisoned.

	m.inner[key] = val
	return val
}
