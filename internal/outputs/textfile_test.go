package outputs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/state"
)

type handledEvt struct {
	wrote bool
	err   error
}

// startText runs the text file output and returns a channel with one event
// per handled state, plus a stop function that returns Run's result.
func startText(t *testing.T, src Source, cfg config.TextFile) (<-chan handledEvt, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	evts := make(chan handledEvt, 100)
	res := make(chan error, 1)
	go func() {
		res <- runTextFile(ctx, src, cfg, quietLog, func(w bool, err error) { evts <- handledEvt{w, err} })
	}()
	var once bool
	stop := func() error {
		if once {
			return nil
		}
		once = true
		cancel()
		select {
		case err := <-res:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("RunTextFile did not return after cancel")
			return nil
		}
	}
	t.Cleanup(func() { _ = stop() })
	return evts, stop
}

func next(t *testing.T, ch <-chan handledEvt) handledEvt {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the output to handle a state")
		return handledEvt{}
	}
}

func readStr(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTextFileDefaultFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "now.txt")
	src := newFakeSource(trackState(1, "New Order", "Blue Monday"))
	evts, stop := startText(t, src, config.TextFile{Path: path})

	if e := next(t, evts); !e.wrote || e.err != nil {
		t.Fatalf("initial: %+v", e)
	}
	if got := readStr(t, path); got != "New Order - Blue Monday" {
		t.Fatalf("initial file = %q", got)
	}

	live := state.State{Mode: state.ModeLive, Performer: str("Synth Person"), Seq: 2}
	idle := state.State{Mode: state.ModeIdle, Performer: str("ignored"), Seq: 3}
	liveNoName := state.State{Mode: state.ModeLive, Seq: 4}
	trackNil := state.State{Mode: state.ModeTrack, Seq: 5}
	for i, c := range []struct {
		st   state.State
		want string
	}{
		{trackState(10, "Only Artist", ""), "Only Artist"},
		{trackState(11, "", "Only Title"), "Only Title"},
		{live, "Synth Person"},
		{idle, ""},
		{liveNoName, ""},
		{trackState(12, "Ünï", "x"), "Ünï - x"}, // default does not fold
		{trackNil, ""},
	} {
		src.set(c.st)
		e := next(t, evts)
		if e.err != nil {
			t.Fatalf("case %d: %v", i, e.err)
		}
		if got := readStr(t, path); got != c.want {
			t.Errorf("case %d: file = %q, want %q", i, got, c.want)
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestTextFileCustomAndASCIIFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "now.txt") // parent is created
	src := newFakeSource(trackState(1, "Björk", "Hyperballad – Ünplugged"))
	cfg := config.TextFile{Path: path, Format: `{{if .Track}}{{.Track.Artist | ascii | upper}} // {{ascii .Track.Title | lower}}{{end}} [{{.EventName}}]{{"\n"}}`}
	evts, _ := startText(t, src, cfg)
	if e := next(t, evts); e.err != nil {
		t.Fatal(e.err)
	}
	if got, want := readStr(t, path), "BJORK // hyperballad - unplugged [801 something]\n"; got != want {
		t.Errorf("file = %q, want %q", got, want)
	}
}

func TestASCIIFold(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"plain", "plain"},
		{"Café Crème Brûlée", "Cafe Creme Brulee"},
		{"ÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖØÙÚÛÜÝ", "AAAAAACEEEEIIIINOOOOOOUUUUY"},
		{"àáâãäåçèéêëìíîïñòóôõöøùúûüýÿ", "aaaaaaceeeeiiiinoooooouuuuyy"},
		{"Æsir ÆØ æ œ Œ ß Þor þ Ð ð", "AEsir AEO ae oe OE ss Thor th D d"},
		{"Łódź Žižek Čapek Şahin Ðorđe", "Lodz Zizek Capek Sahin Dorde"},
		{"ĀāĂăĄąĆćĈĉĊċČčĎďĐđ", "AaAaAaCcCcCcCcDdDd"},
		{"ĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħ", "EeEeEeEeEeGgGgGgGgHhHh"},
		{"ĨĩĪīĬĭĮįİıĲĳĴĵĶķĸ", "IiIiIiIiIiIJijJjKkk"},
		{"ĹĺĻļĽľĿŀŁłŃńŅņŇňŉŊŋ", "LlLlLlLlLlNnNnNnnNGng"},
		{"ŌōŎŏŐőŒœŔŕŖŗŘřŚśŜŝŞşŠš", "OoOoOoOEoeRrRrRrSsSsSsSs"},
		{"ŢţŤťŦŧŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽžſ", "TtTtTtUuUuUuUuUuUuWwYyYZzZzZzs"},
		{"“quoted” ‘it’s’ – — …", `"quoted" 'it's' - - ...`},
		{"a b", "a b"},
		{"é", "e"}, // combining mark dropped
		{"日本語 ★ DJ 🎧", "  DJ "},
		{"tab\there\nnl\x00\x07", "tab\there\nnl"},
		{"× ÷", "x "},
	} {
		if got := ASCII(c.in); got != c.want {
			t.Errorf("ASCII(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if len(strings.Fields(latin1)) != 0x100-0xC0 || len(strings.Fields(latinExtA)) != 0x80 {
		t.Fatalf("table sizes: %d, %d", len(strings.Fields(latin1)), len(strings.Fields(latinExtA)))
	}
}

func TestTextFileAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "now.txt")
	src := newFakeSource(trackState(1, "A", "One"))
	evts, _ := startText(t, src, config.TextFile{Path: path})
	next(t, evts)
	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// An open reader of the old file keeps seeing complete old content: the
	// update replaces the directory entry rather than rewriting in place.
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	src.set(trackState(2, "A", "Two"))
	next(t, evts)
	fi2, _ := os.Stat(path)
	if os.SameFile(fi1, fi2) {
		t.Error("file was rewritten in place, not replaced via rename")
	}
	buf := make([]byte, 100)
	n, _ := old.Read(buf)
	if string(buf[:n]) != "A - One" {
		t.Errorf("old handle saw %q", buf[:n])
	}
	if fi2.Mode().Perm() != 0o644 {
		t.Errorf("mode = %v", fi2.Mode())
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Errorf("temp files left behind: %v", ents)
	}
}

func TestTextFileSkipsUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "now.txt")
	src := newFakeSource(trackState(1, "A", "T"))
	evts, _ := startText(t, src, config.TextFile{Path: path})
	if e := next(t, evts); !e.wrote {
		t.Fatal("first write expected")
	}
	fi1, _ := os.Stat(path)

	// Different seq and history, same rendering.
	st := trackState(2, "A", "T")
	st.History = append(st.History, state.Track{Artist: "old"})
	src.set(st)
	if e := next(t, evts); e.wrote || e.err != nil {
		t.Fatalf("expected skip, got %+v", e)
	}
	fi2, _ := os.Stat(path)
	if !os.SameFile(fi1, fi2) || !fi1.ModTime().Equal(fi2.ModTime()) {
		t.Error("file was touched")
	}

	src.set(trackState(3, "A", "T2"))
	if e := next(t, evts); !e.wrote {
		t.Fatal("changed content must be written")
	}
}

func TestTextFileRewritesStaleFileOnStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "now.txt")
	os.WriteFile(path, []byte("A - T"), 0o644) // same content as will be rendered
	src := newFakeSource(trackState(1, "A", "T"))
	evts, _ := startText(t, src, config.TextFile{Path: path})
	if e := next(t, evts); !e.wrote {
		t.Error("start always writes")
	}
}

func TestTextFileInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "now.txt")
	src := newFakeSource(trackState(1, "A", "T"))
	for _, f := range []string{"{{.Track.Artist", "{{if}}", "{{nosuchfunc .Mode}}", "{{end}}"} {
		err := RunTextFile(context.Background(), src, config.TextFile{Path: path, Format: f}, quietLog)
		if err == nil || !strings.Contains(err.Error(), "invalid format") {
			t.Errorf("format %q: err = %v", f, err)
		}
	}
	if err := RunTextFile(context.Background(), src, config.TextFile{}, quietLog); err == nil {
		t.Error("empty path should fail")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("file must not be written for invalid config")
	}
}

func TestTextFileWriteAndRenderErrorsAreNonFatal(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	os.WriteFile(blocker, nil, 0o644)
	src := newFakeSource(trackState(1, "A", "T"))
	evts, stop := startText(t, src, config.TextFile{Path: filepath.Join(blocker, "now.txt")})
	if e := next(t, evts); e.err == nil {
		t.Fatal("expected write error")
	}
	src.set(trackState(2, "A", "T2"))
	if e := next(t, evts); e.err == nil {
		t.Fatal("expected write error again (retry)")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// Runtime render error (nil pointer): logged, file kept.
	path := filepath.Join(dir, "now.txt")
	src2 := newFakeSource(trackState(1, "A", "T"))
	evts2, _ := startText(t, src2, config.TextFile{Path: path, Format: "{{.Track.Artist}}"})
	next(t, evts2)
	src2.set(state.State{Mode: state.ModeIdle, Seq: 2})
	if e := next(t, evts2); e.err == nil {
		t.Error("expected render error")
	}
	if got := readStr(t, path); got != "A" {
		t.Errorf("file = %q", got)
	}
}

func TestTextFileStopsOnContextCancel(t *testing.T) {
	src := newFakeSource(trackState(1, "A", "T"))
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		res <- RunTextFile(ctx, src, config.TextFile{Path: filepath.Join(t.TempDir(), "n.txt")}, quietLog)
	}()
	cancel()
	select {
	case err := <-res:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not stop")
	}
}
