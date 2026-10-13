// Package health is the shared shape for component status on GET /health.
package health

// Status is one component's health.
type Status struct {
	// OK is false when the component needs attention.
	OK bool `json:"ok"`
	// Warnings are human-readable problems, e.g. "disk low: 1.2 GiB free".
	Warnings []string `json:"warnings,omitempty"`
	// Detail is component-specific JSON-serialisable status.
	Detail any `json:"detail,omitempty"`
}

// Checker is implemented by components that report health.
type Checker interface {
	Health() Status
}

// CheckerFunc adapts a function to Checker.
type CheckerFunc func() Status

func (f CheckerFunc) Health() Status { return f() }
