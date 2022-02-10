package query

import (
	"reflect"
	"sync"

	"github.com/juju/errors"
)

// ReflectCache caches the types for faster look up times.
type ReflectCache struct {
	mutex sync.RWMutex
	cache map[reflect.Type]ReflectStruct
}

// NewReflectCache creates a new ReflectCache that caches the types for faster
// look up times.
func NewReflectCache() ReflectCache {
	return ReflectCache{
		cache: make(map[reflect.Type]ReflectStruct),
	}
}

// Reflect will return a Reflectstruct of a given type.
func (r *ReflectCache) Reflect(value interface{}) (ReflectStruct, error) {
	v := reflect.Indirect(reflect.ValueOf(value))
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if rs, ok := r.cache[v.Type()]; ok {
		return rs, nil
	}

	rs, err := Reflect(v)
	if err != nil {
		return ReflectStruct{}, errors.Trace(err)
	}
	r.cache[v.Type()] = rs
	return rs, nil
}
