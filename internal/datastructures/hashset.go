package datastructures

type HashSet[T string | int] map[T]struct{}

func (set HashSet[T]) Add(element T) {
	set[element] = struct{}{}
}

func (set HashSet[T]) Contains(element T) bool {
	_, exists := set[element]
	return exists
}

func (set HashSet[T]) Remove(element T) {
	delete(set, element)
}
