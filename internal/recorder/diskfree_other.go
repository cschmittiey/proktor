//go:build !(linux || darwin)

package recorder

// statfsFree is not implemented on this platform.
func statfsFree(path string) (uint64, error) {
	return 0, ErrUnsupported
}
