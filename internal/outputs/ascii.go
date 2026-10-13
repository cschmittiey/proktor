package outputs

import "strings"

// latin1 maps U+00C0..U+00FF; "_" means "drop".
const latin1 = "A A A A A A AE C E E E E I I I I D N O O O O O x O U U U U Y Th ss " +
	"a a a a a a ae c e e e e i i i i d n o o o o o _ o u u u u y th y"

// latinExtA maps U+0100..U+017F, eight entries per row.
const latinExtA = "A a A a A a C c " + // 0100
	"C c C c C c D d " + // 0108
	"D d E e E e E e " + // 0110
	"E e E e G g G g " + // 0118
	"G g G g H h H h " + // 0120
	"I i I i I i I i " + // 0128
	"I i IJ ij J j K k " + // 0130 (İ→I, ı→i)
	"k L l L l L l L " + // 0138 (ĸ→k)
	"l L l N n N n N " + // 0140
	"n n NG ng O o O o " + // 0148 (ŉ→n)
	"O o OE oe R r R r " + // 0150
	"R r S s S s S s " + // 0158
	"S s T t T t T t " + // 0160
	"U u U u U u U u " + // 0168
	"U u U u W w Y y " + // 0170
	"Y Z z Z z Z z s " // 0178 (ſ→s)

// asciiFold maps runes outside ASCII to their ASCII replacement.
var asciiFold = buildFold()

func buildFold() map[rune]string {
	m := map[rune]string{
		' ': " ", // no-break space
		'‘': "'", '’': "'", '‚': "'", '′': "'",
		'“': `"`, '”': `"`, '„': `"`, '″': `"`,
		'‐': "-", '‑': "-", '‒': "-", '–': "-", '—': "-", '―': "-",
		'…': "...",
		'«': "<<", '»': ">>",
	}
	for i, tok := range strings.Fields(latin1) {
		m[rune(0xC0+i)] = foldTok(tok)
	}
	for i, tok := range strings.Fields(latinExtA) {
		m[rune(0x100+i)] = foldTok(tok)
	}
	return m
}

func foldTok(t string) string {
	if t == "_" {
		return ""
	}
	return t
}

// ASCII folds s to plain ASCII: Latin-1 and Latin Extended-A letters lose
// their diacritics (é→e, Ø→O, ß→ss, Æ→AE), common typographic punctuation is
// replaced by its ASCII look-alike, and any other non-ASCII rune is dropped.
// Control characters other than tab and newline are dropped too.
func ASCII(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\t' || r == '\n' || (r >= 0x20 && r < 0x7f):
			b.WriteRune(r)
		case r < 0x80:
			// other control characters: drop
		default:
			b.WriteString(asciiFold[r])
		}
	}
	return b.String()
}
