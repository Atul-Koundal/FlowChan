package result

import (
	"errors"
	"testing"
)

func TestCatch_ConvertsPanic(t *testing.T) {
	_, err := Catch(func() (int, error) { panic("boom") })
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PanicError, got %v", err)
	}
	if pe.Value != "boom" || len(pe.Stack) == 0 {
		t.Fatalf("bad PanicError: %+v", pe)
	}
}

func TestCatch_PassesThroughValueAndError(t *testing.T) {
	v, err := Catch(func() (int, error) { return 7, nil })
	if v != 7 || err != nil {
		t.Fatalf("got %v, %v", v, err)
	}
	want := errors.New("x")
	if err := CatchErr(func() error { return want }); err != want {
		t.Fatalf("got %v", err)
	}
}

func TestPanicError_UnwrapsErrorValues(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := CatchErr(func() error { panic(sentinel) })
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is should see through panic(err): %v", err)
	}
}
