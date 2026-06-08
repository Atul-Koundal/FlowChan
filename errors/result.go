package errors

// Result carries either a value or an error from a concurrent operation.
type Result[T any] struct {
    Value T
    Err   error
}

// IsErr returns true if the result holds an error.
func (r Result[T]) IsErr() bool {
    return r.Err != nil
}

// Unwrap returns the value and error together.
func (r Result[T]) Unwrap() (T, error) {
    return r.Value, r.Err
}

// Map applies fn to the value if no error, otherwise passes the error through.
func Map[T, U any](r Result[T], fn func(T) U) Result[U] {
    if r.Err != nil {
        return Result[U]{Err: r.Err}
    }
    return Result[U]{Value: fn(r.Value)}
}

// Collect separates a slice of results into values and errors.
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