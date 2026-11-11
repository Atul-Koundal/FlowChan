package result

import (
	"fmt"
	"runtime/debug"
)

// PanicError is the error produced when user-supplied code panics inside
// a FlowChan worker. It keeps the recovered value and the goroutine stack
// so the failure can be debugged instead of just logged as a string.
type PanicError struct {
	Value any    // the value passed to panic()
	Stack []byte // stack trace captured at the point of recovery
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// Unwrap returns the panic value if it was an error, so errors.Is and
// errors.As work through a recovered panic(err).
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// Catch runs fn and converts a panic into a *PanicError. Every place in
// FlowChan that calls user code goes through Catch (or CatchErr), so a
// misbehaving callback can never crash the host process.
func Catch[T any](fn func() (T, error)) (val T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn()
}

// CatchErr is Catch for functions that return only an error.
func CatchErr(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn()
}
