package result

import "fmt"

// StageError is returned when a pipeline stage fails processing an item.
// It carries the stage name, the item that failed, which attempt it was,
// and the underlying error.
type StageError struct {
	Stage   string // name of the stage where the error occurred
	Attempt int    // which attempt failed (1-based)
	Err     error  // the underlying error
}

func (e *StageError) Error() string {
	return fmt.Sprintf("stage %q failed on attempt %d: %v", e.Stage, e.Attempt, e.Err)
}

// Unwrap returns the underlying error for use with errors.Is and errors.As.
func (e *StageError) Unwrap() error { return e.Err }

// PoolError is returned when a pool task fails after all retries.
// It carries the task type name and the last error returned.
type PoolError struct {
	TaskType string // reflect name of the task type
	Attempts int    // total attempts made
	Err      error  // last error returned
}

func (e *PoolError) Error() string {
	return fmt.Sprintf("task %q failed after %d attempts: %v", e.TaskType, e.Attempts, e.Err)
}

// Unwrap returns the underlying error.
func (e *PoolError) Unwrap() error { return e.Err }

// RetryableError signals that an error is temporary and the operation
// should be retried. Wrap transient errors with this to distinguish
// them from permanent failures.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable: %v", e.Err)
}

func (e *RetryableError) Unwrap() error { return e.Err }

// IsRetryable reports whether err is or wraps a RetryableError.
func IsRetryable(err error) bool {
	var r *RetryableError
	return As(err, &r)
}