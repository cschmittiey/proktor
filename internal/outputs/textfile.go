// Package outputs implements proktor's push outputs driven by state changes:
// the now-playing text file and outgoing webhooks.
package outputs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/state"
)

// Source is satisfied by *state.Machine.
type Source interface {
	Snapshot() state.State
	Subscribe() (<-chan state.State, func())
}

// DefaultTextFormat is the text/template used when TextFile.Format is empty.
//
//   - track mode: "Artist - Title" (just the one that is present if the other
//     is empty, nothing if the track is missing)
//   - live mode: the performer name
//   - idle mode: the empty string
//
// The output has no trailing newline.
const DefaultTextFormat = `{{if eq .Mode "track"}}` +
	`{{with .Track}}{{if and .Artist .Title}}{{.Artist}} - {{.Title}}{{else}}{{.Artist}}{{.Title}}{{end}}{{end}}` +
	`{{else if eq .Mode "live"}}` +
	`{{with .Performer}}{{.}}{{end}}` +
	`{{end}}`

// templateFuncs are available to text file formats.
//
//	ascii  fold to ASCII (see ASCII)
//	upper  strings.ToUpper
//	lower  strings.ToLower
var templateFuncs = template.FuncMap{
	"ascii": ASCII,
	"upper": strings.ToUpper,
	"lower": strings.ToLower,
}

// RunTextFile writes the rendered state to cfg.Path on start and on every
// change until ctx is done. The format (cfg.Format, or DefaultTextFormat) is a
// Go text/template executed against state.State; an invalid template or an
// empty path is returned as an error before anything is written. Write and
// render failures afterwards are logged and retried on the next change.
// Writes are atomic (temp file in the same directory + rename) and skipped
// when the rendered text is unchanged.
func RunTextFile(ctx context.Context, src Source, cfg config.TextFile, logger *slog.Logger) error {
	return runTextFile(ctx, src, cfg, logger, nil)
}

// runTextFile is RunTextFile with a test hook called after every state is
// handled (wrote reports whether the file was rewritten).
func runTextFile(ctx context.Context, src Source, cfg config.TextFile, logger *slog.Logger, handled func(wrote bool, err error)) error {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Path == "" {
		return errors.New("textfile: path is required")
	}
	format := cfg.Format
	if format == "" {
		format = DefaultTextFormat
	}
	tmpl, err := template.New("textfile").Funcs(templateFuncs).Parse(format)
	if err != nil {
		return fmt.Errorf("textfile %s: invalid format: %w", cfg.Path, err)
	}

	// Subscribe before taking the snapshot so no change is missed.
	ch, cancel := src.Subscribe()
	defer cancel()

	var last *string
	handle := func(st state.State) {
		wrote, err := writeRendered(tmpl, st, cfg.Path, &last)
		if err != nil {
			logger.Error("textfile: update failed", "path", cfg.Path, "err", err)
		}
		if handled != nil {
			handled(wrote, err)
		}
	}
	handle(src.Snapshot())
	for {
		select {
		case <-ctx.Done():
			return nil
		case st, ok := <-ch:
			if !ok {
				return nil
			}
			handle(st)
		}
	}
}

func writeRendered(tmpl *template.Template, st state.State, path string, last **string) (bool, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, st); err != nil {
		return false, fmt.Errorf("render: %w", err)
	}
	out := buf.String()
	if *last != nil && **last == out {
		return false, nil
	}
	if err := writeFileAtomic(path, []byte(out)); err != nil {
		return false, err
	}
	*last = &out
	return true, nil
}

// writeFileAtomic replaces path with data via a temp file in the same
// directory, so readers never see a partial file.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(name, 0o644)
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err != nil {
		_ = os.Remove(name)
	}
	return err
}
