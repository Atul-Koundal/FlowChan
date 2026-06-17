# Contributing to FlowChan

Thank you for your interest in contributing. This document explains how to get started, what we look for in contributions, and how to submit changes.

## Getting started

Fork the repository and clone it locally:

    git clone https://github.com/Atul-Koundal/FlowChan
    cd FlowChan
    go mod download

Verify everything passes before making any changes:

    go test ./... -race -timeout 30s

## What we are looking for

Good contributions:
- Bug fixes with a test that reproduces the bug
- Performance improvements with benchmark evidence
- New primitives that are generic and widely applicable
- Documentation improvements and godoc comments
- Additional test coverage, especially edge cases

What to avoid:
- Highly specialized features that only solve one narrow use case
- External dependencies - FlowChan has zero and we want to keep it that way
- Breaking changes to existing public APIs without discussion first
- Functions that are easy to misuse without documentation making it clear

If you are unsure whether something is a good fit, open an issue first to discuss the idea before writing code.

## Development rules

Every new concurrent operation must:
- Accept context.Context as the first parameter
- Accept input as <-chan T
- Return output as <-chan errors.Result[T]
- Close the output channel when input is exhausted or context is done
- Track goroutines with a WaitGroup, not items
- Wrap every user-provided function call in a defer recover()
- Add a ctx.Done() case to every select that could block

Every new package must:
- Have a package-level godoc comment in one file
- Have godoc comments on every exported symbol
- Have a _test.go file with goleak.VerifyTestMain
- Pass go test ./... -race -timeout 30s cleanly
- Import nothing from FlowChan except errors/

## Writing tests

Tests must cover at minimum:
- Happy path - correct output for valid input
- Error propagation - errors flow through correctly
- Cancellation - context cancellation exits without goroutine leaks
- Empty input - no panics or hangs on zero items

Run tests with the race detector always:

    go test ./... -race -timeout 30s

Goleak is already wired into TestMain in every package. Just run the tests normally and goleak will report any leaks automatically.

Run benchmarks before and after performance related changes:

    go test ./benchmarks/... -bench=. -benchmem -count=3

## Submitting a pull request

1. Create a branch from main
2. Make your changes
3. Add or update tests
4. Run go test ./... -race -timeout 30s and confirm it passes
5. Run go vet ./... and fix any issues
6. Write a clear PR description explaining what the change does and why
7. Reference any related issues

## Commit messages

Keep commit messages short and descriptive:

    pool: add WithTimeout option for per-task deadlines
    pipeline: fix Chain dropping errors from first stage
    batch: add NewRealtime sliding window batcher
    docs: add ARCHITECTURE.md

Format is package: what changed. For changes that span multiple packages use all: description.

## Code style

Follow standard Go conventions. Run gofmt before committing:

    gofmt -w .

No linter is enforced but go vet ./... must pass clean.

## Questions

Open an issue with the question label if you are unsure about anything before starting work.