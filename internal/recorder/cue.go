package recorder

import (
	"fmt"
	"strings"
	"time"
)

// trackRec is one detected track as recorded in the sidecars.
type trackRec struct {
	Artist      string
	Title       string
	Comments    []string
	OffsetBytes int64
	Offset      time.Duration // audio time from the start of the file
	DetectedAt  time.Time
}

type cueSheet struct {
	Performer string
	Title     string
	File      string // base name of the .ogg
	Tracks    []trackRec
}

// cueQuote wraps s in double quotes. CUE has no formal escape syntax, so a
// literal quote is backslash-escaped (as most tools accept) and line breaks
// are folded to spaces so a value can never break out of its line.
func cueQuote(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' {
			return ' '
		}
		return r
	}, s)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

// cueTime formats d as MM:SS:FF (75 frames per second). Minutes may exceed 99.
func cueTime(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	frames := int64(d) * 75 / int64(time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", frames/(75*60), frames/75%60, frames%75)
}

func (c cueSheet) render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "PERFORMER %s\n", cueQuote(c.Performer))
	fmt.Fprintf(&b, "TITLE %s\n", cueQuote(c.Title))
	fmt.Fprintf(&b, "FILE %s OGG\n", cueQuote(c.File))
	for i, t := range c.Tracks {
		fmt.Fprintf(&b, "  TRACK %02d AUDIO\n", i+1)
		fmt.Fprintf(&b, "    TITLE %s\n", cueQuote(t.Title))
		fmt.Fprintf(&b, "    PERFORMER %s\n", cueQuote(t.Artist))
		fmt.Fprintf(&b, "    INDEX 01 %s\n", cueTime(t.Offset))
	}
	return b.String()
}
