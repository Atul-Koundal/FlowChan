package errors

import (
    "fmt"
    "testing"
)

func TestResult_IsErr(t *testing.T) {
    ok := Result[int]{Value: 42}
    if ok.IsErr() {
        t.Error("expected no error")
    }

    bad := Result[int]{Err: fmt.Errorf("failed")}
    if !bad.IsErr() {
        t.Error("expected error")
    }
}

func TestResult_Unwrap(t *testing.T) {
    r := Result[string]{Value: "hello"}
    val, err := r.Unwrap()
    if err != nil || val != "hello" {
        t.Errorf("unexpected: %v %v", val, err)
    }
}

func TestMap(t *testing.T) {
    r := Result[int]{Value: 4}
    mapped := Map(r, func(v int) string {
        return fmt.Sprintf("id-%d", v)
    })
    if mapped.Value != "id-4" {
        t.Errorf("unexpected: %v", mapped.Value)
    }
}

func TestMap_PropagatesError(t *testing.T) {
    r := Result[int]{Err: fmt.Errorf("bad")}
    mapped := Map(r, func(v int) string { return "never" })
    if !mapped.IsErr() {
        t.Error("expected error to propagate")
    }
}

func TestCollect(t *testing.T) {
    results := []Result[int]{
        {Value: 1},
        {Err: fmt.Errorf("oops")},
        {Value: 3},
    }
    values, errs := Collect(results)
    if len(values) != 2 || len(errs) != 1 {
        t.Errorf("unexpected split: %v %v", values, errs)
    }
}