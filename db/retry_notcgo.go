// +build !cgo

package db

// isErrorRetryable returns true if the given error might be transient and the
// interaction can be safely retried.
func isErrorRetryable(err error) bool {
	return false
}
