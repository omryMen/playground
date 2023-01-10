package drivers

// Code generated by github.com/launchdarkly/go-options.  DO NOT EDIT.

import (
	"fmt"
	"time"
)

import "github.com/google/go-cmp/cmp"

type ApplyOptionFunc func(c *Options) error

func (f ApplyOptionFunc) apply(c *Options) error {
	return f(c)
}

func applyOptionsOptions(c *Options, options ...Option) error {
	for _, o := range options {
		if err := o.apply(c); err != nil {
			return err
		}
	}
	return nil
}

type Option interface {
	apply(*Options) error
}

type withMultiGetModeImpl struct {
	o MGetMode
}

func (o withMultiGetModeImpl) apply(c *Options) error {
	c.MultiGetMode = o.o
	return nil
}

func (o withMultiGetModeImpl) Equal(v withMultiGetModeImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withMultiGetModeImpl) String() string {
	name := "WithMultiGetMode"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

// WithMultiGetMode MultiGetMode is as strategy we want to use for multi get operation.
func WithMultiGetMode(o MGetMode) Option {
	return withMultiGetModeImpl{
		o: o,
	}
}

type withSlotImpl struct {
	o string
}

func (o withSlotImpl) apply(c *Options) error {
	c.Slot = o.o
	return nil
}

func (o withSlotImpl) Equal(v withSlotImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withSlotImpl) String() string {
	name := "WithSlot"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

// WithSlot Used and required if multi get mode equals MultiGetModeTheSameSlot.
func WithSlot(o string) Option {
	return withSlotImpl{
		o: o,
	}
}

type withPoolSizeImpl struct {
	o uint
}

func (o withPoolSizeImpl) apply(c *Options) error {
	c.PoolSize = o.o
	return nil
}

func (o withPoolSizeImpl) Equal(v withPoolSizeImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withPoolSizeImpl) String() string {
	name := "WithPoolSize"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

// WithPoolSize PoolSize is size of pool for redis connections.
func WithPoolSize(o uint) Option {
	return withPoolSizeImpl{
		o: o,
	}
}

type withTimeoutImpl struct {
	o time.Duration
}

func (o withTimeoutImpl) apply(c *Options) error {
	c.Timeout = o.o
	return nil
}

func (o withTimeoutImpl) Equal(v withTimeoutImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withTimeoutImpl) String() string {
	name := "WithTimeout"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

// WithTimeout Timeout will be used as read/write timeout for opened redis connection.
func WithTimeout(o time.Duration) Option {
	return withTimeoutImpl{
		o: o,
	}
}