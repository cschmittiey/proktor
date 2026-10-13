# proktor

A small box that sits on the event network between the DJ booth and the
outside world. DJs point Traktor's built-in broadcast at it, and proktor:

1. pulls the now-playing track out of the stream metadata,
2. exposes it over a small HTTP API that displays (like a vintage CRT
   terminal at the event) can subscribe to,
3. relays the audio upstream to an Icecast / AzuraCast station (e.g. `radio.hack.bike`),
4. records the whole night to disk with a tracklist.

The name is proxy + traktor.

## Background

This came out of a chat about an event (the "801" crew). One person has an
old CRT console with nothing to do; the idea was a live "NOW PLAYING" display
on the TTY. Problems raised in the chat:

- Traktor has no clean "write the current track to a file / webhook" feature.
  The only reliable way to get live track info out is its **broadcast**
  feature, which acts as an Icecast source client and embeds metadata in the
  stream. A tiny Icecast-pretending server can read it
  (prior art: [radusuciu/traktor_nowplaying](https://github.com/radusuciu/traktor_nowplaying)).
- Nothing existing does both "read the metadata" **and** "forward the stream
  somewhere real". You get one or the other.
- Traktor and AzuraCast auth don't get along well (Traktor only sends a
  password, AzuraCast streamers want username + password), so live sets to
  `radio.hack.bike/public/devfm` are fiddly. proktor can sit in the middle
  and own the upstream credentials so DJs never touch them.
- Traktor's metadata flips to the new track as soon as the second deck starts
  playing, so if a DJ starts the next track early (cueing / long blend) the
  display is wrong for a while.
- Not everyone is on Traktor. The night has multiple gear setups (Pioneer
  hardware) and live synth performances with no track metadata at all.
- Setup has to be near-zero for DJs: "turn on broadcast with these settings"
  and nothing more. Anything harder won't happen.

## Goals

- **G1** – Accept a Traktor broadcast with nothing on the DJ side beyond
  entering host/port/mount/password in Traktor's Broadcasting prefs.
- **G2** – Extract artist/title on every track change, with no audio
  re-encoding (pure passthrough of the bytes).
- **G3** – Expose now-playing state over a simple, stable HTTP API (JSON +
  SSE + plain text) that anyone can build a display on, including the CRT.
- **G4** – Relay the stream to zero or more upstream Icecast-compatible
  servers using credentials held by proktor.
- **G5** – Record every set to disk with a timestamped tracklist.
- **G6** – Have a manual path for everything Traktor can't cover (other gear,
  live acts, wrong metadata), usable from a phone in a dark room.
- **G7** – Survive the night: upstream drops, DJs disconnecting/reconnecting,
  handoffs between DJs, all without restarting anything.

## Non-goals (for v1)

- Running a public listener-facing Icecast server. Upstream (AzuraCast etc.)
  does that.
- Transcoding, mixing, or any DSP.
- Audio fingerprinting / Shazam-style ID (see Future).
- Pioneer Pro DJ Link integration (see Future).
- Anything that requires installing software on the DJ's laptop.
- Driving any display, the CRT included. Displays are API clients built and
  owned by whoever runs them; proktor only ships a reference example.

## Architecture

```
                 ┌──────────────────────── proktor ─────────────────────────┐
 Traktor  ──────►│ ingest (Icecast source server)                           │
 (SOURCE/PUT,    │    │  raw bytes                                          │
  Ogg Vorbis)    │    ├──────────────► relay ──────────────► upstream(s)    │──► AzuraCast / Icecast
                 │    ├──────────────► recorder ──► ./recordings/*.ogg      │
                 │    └─► ogg parser ─► metadata ─┐                         │
                 │                                ▼                         │
 phone / curl ──►│ HTTP API (manual, webhook) ─► track state ──► outputs    │
                 │                              (debounce,       ├─ HTTP API ──► displays (CRT etc.)
                 │                               overrides)      ├─ control page
                 │                                               ├─ text file (OBS etc.)
                 │                                               ├─ webhooks out
                 │                                               └─ tracklist log
                 └──────────────────────────────────────────────────────────┘
```

Single process, single static binary, one config file. Components talk
through plain channels / an in-process event bus; no external broker.

### Ingest

- Listen on a TCP port and speak enough of the Icecast **source** protocol
  for Traktor: accept `SOURCE /<mount> HTTP/1.0` (legacy) and
  `PUT /<mount> HTTP/1.1` (Icecast 2.4+), HTTP Basic auth, reply
  `HTTP/1.0 200 OK`, then read the body as a continuous byte stream.
- Auth is per-mount and deliberately loose: a single shared password is fine,
  or none. The event LAN is the trust boundary. Accept any username, since
  Traktor sends `source`.
- One active source per mount. If a second source connects to a busy mount,
  reject with 403 (configurable: `reject` | `takeover`). `takeover` exists so a
  DJ handoff can't get stuck behind a dead connection from the previous laptop.
- Record `ice-name`, `ice-description`, `ice-genre`, etc. headers as
  per-session info. If Traktor's stream name is set to the DJ's name, it can be
  used as the performer name.

### Ogg / metadata parsing

Traktor broadcasts Ogg Vorbis only. On track change it starts a new chained
logical bitstream whose Vorbis comment header carries `ARTIST` and `TITLE`.

- Parse Ogg pages from the byte stream as it passes through (never buffer
  more than a page or two; never modify bytes).
- On every BOS page, reassemble the first packets of the new logical stream,
  find the comment header (packet type `0x03` + `"vorbis"`), and decode the
  comment list.
- Emit a `TrackDetected{artist, title, raw_comments, at}` event.
- Tolerate garbage: if parsing breaks, log it, resync on the next `OggS`
  capture pattern, and keep the audio flowing. Metadata bugs must never
  interrupt audio.

**First implementation task:** capture a real Traktor broadcast to a file
(including a few track changes and a deck overlap) and commit it as a test
fixture. Verify the chained-stream assumption against it before building the
rest on top.

### Track state, debounce, overrides

The "Traktor flips too early" problem lives here.

- Detected tracks don't go live immediately. They wait `metadata_delay`
  (config, default ~0s, suggest 30–60s at events with long blends) before
  becoming "now playing". If another change arrives during the wait, the
  pending one is replaced.
- The tracklist log records **both** the raw detect time and the time it was
  published, so recordings can be cut accurately later even if the display
  was delayed.
- Manual overrides beat automatic data:
  - `set` — show this artist/title now (pins until cleared or until the next
    automatic change, configurable).
  - `performer` — set the current act ("live: <name>"), used for synth sets
    and non-Traktor gear. Shown alongside or instead of track info.
  - `clear` / `skip` — drop a wrong pending/current track.
  - `idle` — nothing playing (between acts).
- State is in memory with a small append-only JSON-lines file so a restart
  mid-event comes back showing the right thing.

### Relay

- For each configured upstream, act as an Icecast source client: connect,
  send `PUT` (or `SOURCE` for old servers) with the upstream's real
  credentials and the right `Content-Type: application/ogg`, and forward the
  bytes as they arrive.
- AzuraCast streamer accounts: support its username/password form, including
  the `user:pass`-in-password-field workaround, so DJs never deal with it.
- Upstream failure must not affect ingest, recording or display. Reconnect
  with backoff. When reconnecting mid-stream, wait for the next Ogg BOS page
  before sending, so the upstream gets a valid stream start (Vorbis needs its
  headers).
- Each upstream has its own bounded buffer; if it falls too far behind, drop
  and reconnect rather than growing memory forever.
- When the source disconnects, disconnect upstream too (default) or keep the
  connection open briefly for a fast DJ handoff (config, `hold_seconds`).

### Recorder

- Write raw bytes to `recordings/<date>/<HHMMSS>-<mount>-<performer>.ogg`,
  one file per source connection. No re-encode: a chained Ogg file plays fine.
- Write a sidecar tracklist (`.tracks.jsonl` plus a `.cue`) with offsets
  from the start of the file.
- Configurable on/off. Warn loudly (log + `/health`) when disk is low.

### Outputs

**HTTP API.** This is the main product surface. proktor doesn't draw
anything itself; the CRT (a serial terminal) and any other display are
separate clients that someone else builds against this API. So the API needs
to be stable, documented, and easy to use from a 20-line script.

Served on its own port, separate from the ingest port. Reads are
unauthenticated (LAN); writes need a bearer token from config.

- `GET /now` → current state as JSON (schema below).
- `GET /now.txt` → the same state as a couple of plain text lines
  (`artist - title` / performer), for the laziest possible client
  (`curl` in a loop, `watch`, a shell script writing to `/dev/ttyUSB0`).
- `GET /events` → Server-Sent Events. On connect, sends the full current
  state immediately, then a `state` event on every change, plus a `:ping`
  comment every 15s so clients can detect a dead connection. This is the
  recommended path for displays: no polling, instant updates.
- `GET /tracks` → tonight's tracklist (JSON; `?format=txt` for plain).
- `GET /health` → source/upstream/recorder status, for a status line on a
  display or for monitoring.
- `POST /now` → manual set / performer / clear / idle (JSON body, token).
- `GET /` → tiny mobile-friendly control page: current state, text fields for
  artist/title/performer, buttons for clear/idle. No build step, one HTML file
  embedded in the binary. It's just another client of the API above.

State object (shared by `/now`, `/events` and webhooks out):

```jsonc
{
  "mode": "track",              // "track" | "live" | "idle"
  "track": {                    // null unless mode == "track"
    "artist": "New Order",
    "title": "Blue Monday",
    "source": "traktor",        // "traktor" | "manual" | "webhook"
    "started_at": "2026-10-13T22:14:05Z"
  },
  "performer": "dj example",    // null if unknown
  "event_name": "801 something",
  "history": [ /* last N tracks, newest first, same shape as track */ ],
  "seq": 42,                    // increments on every change
  "updated_at": "2026-10-13T22:14:05Z"
}
```

Rules for the API contract:

- Times are UTC RFC 3339. Clients compute "elapsed" themselves.
- Strings are UTF-8 as received from Traktor. The API doesn't transliterate;
  a serial terminal client probably needs to fold to ASCII, and that's its
  job. Ship a small example that does this.
- Adding fields is fine; renaming/removing is a breaking change. Version
  the schema with a `"v": 1` field if it ever needs to change.
- Ship one reference client in the repo (`examples/tty.sh` or similar,
  SSE → plain text on a serial port) so the display folks have a working
  starting point. That's an example, not a supported component.

**Text file**: write `now playing` to a file using a format string, for OBS
overlays or anything else that wants a file.

**Webhooks out**: POST the state JSON to configured URLs on every change.
Fire-and-forget with a timeout; failures logged, never block.

### Other inputs

- **Webhook in** (`POST /now` with a source tag): lets any other tool push
  track info. This covers non-Traktor gear in v1 for anyone willing to run a
  script, and is the hook Future adapters plug into.

## Config

One file (TOML), with every value overridable by CLI flag / env for quick
on-site tweaks. Sketch:

```toml
event_name = "801 something"

[ingest]
listen = "0.0.0.0:8000"
[[ingest.mount]]
path = "/live"
password = "traktor"
on_conflict = "takeover"

[metadata]
delay_seconds = 30
manual_pin = "until_next_auto"   # or "until_cleared"

[[upstream]]
name = "devfm"
url = "http://radio.hack.bike:8005/"
mount = "/"
username = "dj_801"
password = "..."
protocol = "put"                 # or "source"
hold_seconds = 20

[recorder]
enabled = true
dir = "./recordings"

[http]
listen = "0.0.0.0:8080"
token = "..."                    # required for POST /now
history_length = 10

[[webhook]]
url = "https://example.invalid/hook"
```

## DJ-facing setup (the whole thing)

What we hand DJs, on a card at the booth:

> Traktor → Preferences → Broadcasting
> Server: `<proktor IP>` · Port: `8000` · Mount: `/live` · Password: `traktor`
> Format: Ogg Vorbis, any bitrate (192 kbps recommended)
> Stream name: your DJ name
> Then hit the broadcast button (the antenna) in the Mixer/Master panel.

If it takes more than that, proktor has failed G1.

## Implementation notes

- Language: **Go** suggested. One static binary, good net/HTTP
  stdlib, easy cross-compile to a Raspberry Pi in the booth. Rust
  is also fine. Avoid anything that needs a runtime installed on the box.
- Nix flake with a `devShell` and a package output; a NixOS module is a nice
  to have for the Pi.
- Pass dependencies (clock, filesystem, network dialer) in explicitly so
  the debounce logic and relay reconnect can be tested without real hardware or real time.
- Tests worth having from day one:
  - Ogg parser against the captured Traktor fixture (track changes detected,
    bytes out == bytes in).
  - Debounce/override state machine with a fake clock.
  - Relay against a fake Icecast server that drops connections.
  - API contract tests: golden JSON for `/now`, SSE sends full state on
    connect and one event per change.
- A `proktor simulate <file.ogg>` command that replays a capture into the
  ingest port in real time, for testing the full pipeline without Traktor.

## Milestones

1. **Sniff** — ingest + Ogg parser, prints tracks to stdout. Capture fixture.
   (Basically `traktor_nowplaying`, but ours.)
2. **API** — debounce, `/now`, `/now.txt`, `/events`, `/health`, plus the
   reference TTY example. At this point the display folks can start.
3. **Manual** — `POST /now`, control page, performer / live-act mode.
4. **Relay** — upstream to AzuraCast, reconnects, handoffs.
5. **Record** — recorder + tracklist/cue sidecars.
6. **Polish** — webhooks, text file output, NixOS module, booth card.

Milestones 1–3 alone are enough for the CRT idea to work at the event, even if
relay and recording slip.

## Future

- **Pioneer Pro DJ Link** adapter: read what's on air from CDJs/XDJs on the
  network (beat-link / prolink-connect style). Covers the Pioneer setups
  without laptops.
- **Audio fingerprint fallback**: mic or line-in, identify unknown tracks
  (cf. [maurocastermans/now-playing](https://github.com/maurocastermans/now-playing)).
  Useful as a last resort, unreliable for underground/unreleased tracks.
- **Smarter early-flip handling**: instead of a fixed delay, use stream
  loudness/energy or a DJ-pressed "commit" hotkey (MIDI mapping) to decide when
  the new track is really on air.
- Local LAN listener mount (so people in other rooms can tune in) without
  going through the internet.
- Pull the track history into a post-event page / archive alongside the
  recordings.

## Open questions

- Does the state schema cover what the CRT folks want to show? Worth
  sending them the schema early, before milestone 2 locks it.
- Which gear setups are confirmed for the event, and how many are Traktor?
- Is relaying to `radio.hack.bike` (devfm) wanted for this event, and which
  station/mount and streamer account?
- Is recording OK with the performers? Needs an opt-out per set.
- What machine runs proktor at the venue (Pi in the booth vs. a laptop), and
  is there wired network to the booth?
