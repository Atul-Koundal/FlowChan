package result

import (
	"errors"
	"fmt"
	"testing"
)

func TestStageError(t *testing.T) {
	underlying := fmt.Errorf("connection refused")
	err := &StageError{
		Stage:   "fetch",
		Attempt: 2,
		Err:     underlying,
	}

	if !errors.Is(err, underlying) {
		t.Error("expected errors.Is to find underlying error")
	}
	expected := `stage "fetch" failed on attempt 2: connection refused`
	if err.Error() != expected {
		t.Errorf("unexpected message: %s", err.Error())
	}
}

func TestPoolError(t *testing.T) {
	underlying := fmt.Errorf("timeout")
	err := &PoolError{
		TaskType: "EmailTask",
		Attempts: 3,
		Err:      underlying,
	}

	if !errors.Is(err, underlying) {
		t.Error("expected errors.Is to find underlying error")
	}
}

func TestRetryableError(t *testing.T) {
	underlying := fmt.Errorf("service unavailable")
	err := &RetryableError{Err: underlying}

	if !IsRetryable(err) {
		t.Error("expected IsRetryable to return true")
	}
	if !errors.Is(err, underlying) {
		t.Error("expected errors.Is to find underlying error")
	}
}

func TestIsRetryable_NonRetryable(t *testing.T) {
	err := fmt.Errorf("permanent failure")
	if IsRetryable(err) {
		t.Error("expected IsRetryable to return false for plain error")
	}
}