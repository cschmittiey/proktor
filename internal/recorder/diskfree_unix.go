//go:build linux || darwin

package recorder

import "syscall"

// statfsFree returns the bytes available to unprivileged users on the
// filesystem holding path.
func statfsFree(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil //nolint:unconvert // field types differ per OS
}
