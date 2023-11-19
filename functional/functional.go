package functional

import (
	"reflect"
	"sync"
)

func MapStrings[T any](list []T, fn func(T) string) []string {
	return Map[T, string](list, func(x T) string {
		return fn(x)
	})
}

func Map[T any, R any](list []T, fn func(T) R) []R {
	return MapWithIndex[T, R](list, func(x T, _ int) R {
		return fn(x)
	})
}

func MapWithIndex[T any, R any](list []T, fn func(T, int) R) []R {
	ret := make([]R, len(list))

	for pos, x := range list {
		ret[pos] = fn(x, pos)
	}

	return ret
}

func Compact[T any](list []T) []T {
	ret := []T{}

	for _, x := range list {
		var c interface{} = x

		if c == nil || (reflect.ValueOf(c).Kind() == reflect.Ptr && reflect.ValueOf(c).IsNil()) {
			continue
		}

		ret = append(ret, x)
	}
	return ret
}

func Filter[T any](list []T, fn func(T) bool) []T {
	ret := []T{}

	for _, x := range list {
		if result := fn(x); result {
			ret = append(ret, x)
		}
	}

	return ret
}

func Find[T any](list []T, fn func(T) bool) *T {
	for _, x := range list {
		if fn(x) {
			return &x
		}
	}
	return nil
}

func DeDup[T any](list []T, fnc func(T) string) []T {
	cache := map[string]any{}
	ret := []T{}

	for _, rec := range list {
		r := fnc(rec)
		if _, ok := cache[r]; !ok {
			ret = append(ret, rec)
			cache[r] = true
		}
	}

	return ret
}

func AsyncMap[T any, R any](list []T, fn func(T) R) []R {
	var wg sync.WaitGroup
	var lock sync.RWMutex

	ret := make([]R, len(list))

	for pos, x := range list {
		wg.Add(1)

		go func(x T, pos int) {
			res := fn(x)

			lock.Lock()

			ret[pos] = res

			lock.Unlock()

			wg.Done()
		}(x, pos)
	}

	wg.Wait()

	return ret
}

func Flatten[T any](list [][]T) []T {
	ret := []T{}

	for _, x := range list {
		ret = append(ret, x...)
	}

	return ret
}

// AsyncFilter runs the given function on each item in the list, asynchronously. filtering out nil results.
func AsyncFilter[T any, R any](list []T, fn func(T) *R) []R {
	var wg sync.WaitGroup
	var lock sync.RWMutex

	ret := []R{}

	for _, x := range list {
		wg.Add(1)

		go func(x T) {
			if res := fn(x); res != nil {
				lock.Lock()
				defer lock.Unlock()

				ret = append(ret, *res)
			}
			wg.Done()
		}(x)
	}

	wg.Wait()

	return ret
}

// AsyncForEach runs the given function on each item in the list, asynchronously.
func AsyncForEach[T any](list []T, fn func(T)) {
	var wg sync.WaitGroup

	for _, x := range list {
		wg.Add(1)

		go func(x T) {
			fn(x)
			wg.Done()
		}(x)
	}

	wg.Wait()
}

// Each runs the given function on each item in the list.
func Each[T any](list []T, fn func(T)) {
	for _, x := range list {
		fn(x)
	}
}

// EachSuccess runs the given function on each item in the list, and returns the first error encountered.
func EachSuccess[T any](list []T, fn func(T) error) error {
	for _, x := range list {
		if err := fn(x); err != nil {
			return err
		}
	}
	return nil
}
