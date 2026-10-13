package recorder

import (
	"strings"
	"unicode"
)

// maxSlugRunes caps the length of each filename component.
const maxSlugRunes = 60

// slug turns arbitrary text into a safe filename component: unicode letters
// and digits are kept, every other rune becomes '-', repeats are collapsed,
// leading/trailing '-' are trimmed and the result is capped at 60 runes.
// If nothing is left, fallback is returned.
func slug(s, fallback string) string {
	var b strings.Builder
	n := 0
	lastDash := true // suppresses a leading '-'
	for _, r := range s {
		if n >= maxSlugRunes {
			break
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			n++
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			n++
			lastDash = true
		}
	}
	out := strings.TrimRight(b.String(), "-")
	if out == "" {
		return fallback
	}
	return out
}
