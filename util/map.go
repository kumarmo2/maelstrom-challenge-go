package util

func ToKeySlice[T comparable, V any](dictionary map[T]V) []T {

	n := len(dictionary)
	slice := make([]T, n)

	i := 0
	for k := range dictionary {
		slice[i] = k
		i++
	}
	return slice
}

func ToValSlice[T comparable, V any](dictionary map[T]V) []V {

	n := len(dictionary)
	slice := make([]V, n)

	i := 0
	for _, v := range dictionary {
		slice[i] = v
		i++
	}
	return slice
}
