// Package result provides the Result type used throughout FlowChan
// to carry a value and an error together safely through channels.
package result

// Result carries either a successful value or an error from a
// concurrent operation. Check IsErr or Err before using Value.
type Result[T any] struct {
	Value T
	Err   error
}

// IsErr reports whether r holds an error.
func (r Result[T]) IsErr() bool {
	return r.Err != nil
}

// Unwrap returns the value and error together.
func (r Result[T]) Unwrap() (T, error) {
	return r.Value, r.Err
}

// Map applies fn to the value if r has no error. If r holds an
// error it is forwarded to the returned Result unchanged.
func Map[T, U any](r Result[T], fn func(T) U) Result[U] {
	if r.Err != nil {
		return Result[U]{Err: r.Err}
	}
	return Result[U]{Value: fn(r.Value)}
}

// Collect splits a slice of Results into successful values and errors.
// Order within each returned slice matches the input.
func Collect[T any](results []Result[T]) ([]T, []error) {
	var values []T
	var errs []error
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		} else {
			values = append(values, r.Value)
		}
	}
	return values, errs
}