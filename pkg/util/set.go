package util

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return make(Set[T])
}

func (s Set[T]) Add(item T) {
	s[item] = struct{}{}
}

func (s Set[T]) Diff(other Set[T]) Set[T] {
	result := NewSet[T]()
	for item := range s {
		if _, found := other[item]; !found {
			result.Add(item)
		}
	}
	return result
}

func (s Set[T]) Union(other Set[T]) Set[T] {
	result := NewSet[T]()
	for item := range s {
		result.Add(item)
	}
	for item := range other {
		result.Add(item)
	}
	return result
}

func (s Set[T]) List() []T {
	result := make([]T, 0, len(s))
	for item := range s {
		result = append(result, item)
	}
	return result
}

// For maintaining unique collections of items via a custom
type AnySet[T any] struct {
	items   []T
	compare func(a, b T) bool
}

func NewAnySet[T any](compare func(a, b T) bool) *AnySet[T] {
	return &AnySet[T]{
		items:   make([]T, 0),
		compare: compare,
	}
}

func (us *AnySet[T]) Add(item T) {
	alreadyPresent := false
	for _, elem := range us.items {
		same := us.compare(item, elem)
		alreadyPresent = alreadyPresent || same
	}
	if !alreadyPresent {
		us.items = append(us.items, item)
	}
}

func (us *AnySet[T]) Items() []T {
	return us.items
}
