# testdata

## chained.ogg -- SYNTHETIC

> **This file is synthetic. It is not a Traktor capture.**
> SPEC.md's first implementation task is to record a *real* Traktor
> broadcast (several track changes plus a deck overlap) and verify the
> chained-stream assumption against it. That still has to be done at a real
> Traktor setup. `chained.ogg` only stands in until then.

`chained.ogg` imitates what Traktor's broadcast is believed to send: five
short Ogg Vorbis logical bitstreams (about 3 s of a sine tone each, 44100 Hz
stereo, libvorbis `-q:a 0`) concatenated back to back. Every stream has its
own BOS page and serial number and carries `ARTIST` and `TITLE` Vorbis
comments, including UTF-8 values and a `=` inside a value:

| # | ARTIST | TITLE |
|---|--------|-------|
| 1 | New Order | Blue Monday |
| 2 | Röyksopp | Eple |
| 3 | 坂本龍一 | Merry Christmas Mr. Lawrence |
| 4 | Aphex Twin | Xtal |
| 5 | DJ Überlong Name feat. Ünïcödé | Track with = equals sign |

Regenerate with `./gen-fixture.sh` (needs ffmpeg with libvorbis). The script
encodes each track separately with bitexact flags and fixed serial numbers
(`-serial_offset`), then joins them with `cat`; chaining is plain
concatenation. The output is deterministic for a given ffmpeg/libvorbis
version, and the committed file is about 54 KB.

## traktor-capture.ogg -- still to be recorded

Record it with proktor's own recorder (it writes the raw bytes exactly as
received) from a real Traktor Broadcasting session, then commit it here as
`testdata/traktor-capture.ogg`. Include a few track changes and at least one
deck overlap.

The Go tests in `internal/ogg` run over every `testdata/*.ogg` automatically.
For each file they check that bytes in equal bytes out and that at least one
track is detected. `chained.ogg` additionally has its exact five
artist/title pairs asserted. Once the real capture is committed it is picked
up with no code change. Add assertions for its expected tracks next to
`TestTestdata` in `internal/ogg/scanner_test.go`.
