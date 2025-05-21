package codec

import (
	"fmt"
)

// Registry manages available codecs
type Registry struct {
	codecs map[string]CodecFunc
}

// CodecFunc transforms a value according to codec rules
type CodecFunc func(interface{}) (interface{}, error)

// NewRegistry creates a new codec registry
func NewRegistry() *Registry {
	return &Registry{
		codecs: make(map[string]CodecFunc),
	}
}

// Register adds a new codec to the registry
func (r *Registry) Register(name string, fn CodecFunc) error {
	if _, exists := r.codecs[name]; exists {
		return fmt.Errorf("codec %s already registered", name)
	}
	r.codecs[name] = fn
	return nil
}

// Get retrieves a codec by name
func (r *Registry) Get(name string) (CodecFunc, error) {
	fn, exists := r.codecs[name]
	if !exists {
		return nil, fmt.Errorf("codec %s not found", name)
	}
	return fn, nil
}

// Apply applies a codec to a value
func (r *Registry) Apply(name string, value interface{}) (interface{}, error) {
	fn, err := r.Get(name)
	if err != nil {
		return nil, err
	}
	return fn(value)
}
