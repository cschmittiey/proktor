# proktor

proktor sits between a DJ's Traktor broadcast and the outside world. Traktor's
built-in broadcast is an Icecast source client, so it can point at proktor
like any stream server. proktor reads the now-playing track out of the stream
metadata, serves it over a small HTTP API for displays, relays the untouched
audio to an Icecast or AzuraCast station, and records the set to disk with a
tracklist. The name is proxy + traktor. It is one static Go binary with one
config file. The design is in [SPEC.md](SPEC.md).

```
Traktor broadcast (Ogg Vorbis)
          │
          ▼
    ┌───────────┐
    │  ingest   │  :8000, Icecast SOURCE/PUT, bytes passed through
    └─────┬─────┘
          │
    ┌─────┼──────────────┐
    ▼     ▼              ▼
 metadata relay      recorder
    │     │              │
    ▼     ▼              ▼
  state  AzuraCast    recordings/
    │    / Icecast
    ▼
 HTTP API :8080 ◄── POST /now (phone, scripts)
    │
    ▼
 displays, scripts, control page
```

## Quick start

Build it, or use Nix:

```sh
go build ./cmd/proktor        # or: nix build . / nix run .
./proktor -token secret
```

For a dev shell with Go, gopls, ffmpeg and jq, run `nix develop`, or
`direnv allow` once to load it on `cd`.

With no config file, defaults apply: ingest on `:8000` with mount `/live` and
no password, HTTP API on `:8080`, no delay, recording off. The binary prints
the DJ booth card with this machine's LAN addresses filled in.

In another terminal, replay a capture as if Traktor were broadcasting:

```sh
./proktor simulate testdata/chained.ogg
curl localhost:8080/now
curl -N localhost:8080/events
```

Then open http://localhost:8080/ for the control page. It asks for the token
once and keeps it in that browser.

## DJ setup

The card for the booth is in [docs/booth-card.md](docs/booth-card.md). The
DJ's side is five settings in Traktor:

```
Traktor → Preferences → Broadcasting
Server:      <proktor IP>
Port:        8000     Mount: /live     Password: traktor
Format:      Ogg Vorbis, 192 kbps
Stream name: your DJ name
```

Then hit the broadcast (antenna) button in the Mixer/Master panel. The
password must match the one proktor is configured with.

## Configuration

Copy the example and edit it. Every key is listed there with its default:

```sh
cp proktor.example.toml proktor.toml
./proktor -config proktor.toml
```

Precedence is defaults < config file < `PROKTOR_*` environment < flags.

Flags for on-site tweaks:

```sh
./proktor -set metadata.delay_seconds=45 -set upstream.0.password=hunter2
```

`-set key=value` is repeatable and works with any key. These shortcuts are
the common ones:

| Flag | Key |
|------|-----|
| `-event` | `event_name` |
| `-ingest` | `ingest.listen` |
| `-http` | `http.listen` |
| `-token` | `http.token` |
| `-delay` | `metadata.delay_seconds` |
| `-password` | `ingest.mount.0.password` |
| `-record` | `recorder.enabled` |
| `-recordings` | `recorder.dir` |

Environment variables are the path upper-cased, with `PROKTOR_` in front and
dots turned into underscores. Array entries are indexed from 0:

```sh
PROKTOR_HTTP_TOKEN=secret PROKTOR_UPSTREAM_0_PASSWORD=hunter2 ./proktor
```

`proktor config` prints the effective config with secrets redacted, followed
by every overridable key. Unknown keys are an error, so typos fail at startup.

## HTTP API

Reads are unauthenticated. Writes need `Authorization: Bearer <token>`.

| Method | Path | What it returns |
|--------|------|-----------------|
| GET | `/now` | Current state as JSON |
| GET | `/now.txt` | Plain text: `artist - title`, then the performer |
| GET | `/events` | Server-Sent Events: state on connect and on every change, `:ping` every 15 s |
| GET | `/tracks` | Tonight's tracklist (JSON; `?format=txt` for plain text) |
| GET | `/health` | Component status (always HTTP 200; check `ok` in the body) |
| POST | `/now` | Manual set / performer / live / clear / skip / idle (needs token) |
| GET | `/` | Mobile control page |

For the full state object, the `/now.txt` format and the contract for building
a display, see [examples/README.md](examples/README.md). For the CRT, see
[examples/tty.sh](examples/tty.sh).

### Manual control (POST /now)

Every action is a JSON body. The token is required.

```sh
# set: show an artist and title now
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' \
  -d '{"action":"set","artist":"New Order","title":"Blue Monday"}'

# performer: set the act name alongside the track (empty string clears it)
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' \
  -d '{"action":"performer","performer":"dj example"}'

# live: a live act. Sets the performer and clears the track.
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' \
  -d '{"action":"live","performer":"synth set"}'

# clear: drop the wrong current track and any pending one
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' -d '{"action":"clear"}'

# skip: drop only the pending track (the one waiting out the delay)
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' -d '{"action":"skip"}'

# idle: between acts. Clears everything.
curl -X POST localhost:8080/now -H "Authorization: Bearer secret" \
  -H 'Content-Type: application/json' -d '{"action":"idle"}'
```

- `set` shows artist and title immediately. How long it sticks is set by
  `metadata.manual_pin`: until the next automatic change (default) or until
  cleared.
- `performer` changes only the performer name and keeps the current track.
- `live` is for acts with no track metadata. The current track goes to history.
- `clear` and `skip` fix a wrong track. `clear` drops the current and pending
  track. `skip` drops only the pending one.
- `idle` is for between acts. It clears the track, the pending track and the
  performer.

Tools that push track info, such as a script watching another player, should
send `"source":"webhook"` with `set`. A webhook `set` goes live immediately and
never pins. With `manual_pin = "until_cleared"`, it's ignored while a manual
track is pinned, and the tracklist records that it was ignored.

## Relay to AzuraCast

Each `[[upstream]]` is a station proktor relays to. The DJs never see its
credentials. An AzuraCast streamer account looks like this:

```toml
[[upstream]]
name = "devfm"
url = "http://radio.hack.bike:8005/"
mount = "/"
username = "dj_801"
auth_style = "user_in_password"
protocol = "put"
hold_seconds = 20
# password from the environment, so it stays out of the file:
#   PROKTOR_UPSTREAM_0_PASSWORD=...
```

AzuraCast's Icecast-compatible port takes only a password, but the streamer
account has a username too. Liquidsoap's harbor reads the username as
`source` and the `user:pass` pair as the password. `auth_style =
"user_in_password"` sends exactly that: Basic auth with user `source` and
password `dj_801:<password>`. Use `"basic"` (the default) for servers that take
a normal username and password.

Only plain `http` upstreams are supported for now.

Upstream trouble never affects ingest, recording or the display. proktor
reconnects with backoff. When it reconnects mid-set, it replays the current
track's Ogg headers before the live audio, so the upstream gets a valid stream
start right away.

`hold_seconds` keeps the upstream connection open after the DJ disconnects, so
the next DJ can take over without the station going off air. The next DJ's
stream starts with a new Ogg BOS page, so it chains onto the held connection.
The station may cut an idle source sooner than `hold_seconds`, though.
Icecast's `<source-timeout>` defaults to 10 seconds, so check the station's
setting. If the connection is cut during the hold, proktor doesn't reconnect
until the next DJ starts.

## Recording

With `recorder.enabled = true` (or `-record true`), each source connection is
written as raw bytes, with no re-encode, to:

```
recordings/<YYYY-MM-DD>/<HHMMSS>-<mount>-<performer>.ogg
recordings/<YYYY-MM-DD>/<HHMMSS>-<mount>-<performer>.tracks.jsonl
recordings/<YYYY-MM-DD>/<HHMMSS>-<mount>-<performer>.cue
```

The `.tracks.jsonl` file is the tracklist with offsets from the start of the
file. The `.cue` file is the same tracklist as a CUE sheet. If a name is
already taken, a `-2`, `-3` suffix is added. A chained Ogg file plays in any
Ogg player. Recording is off by default, so check with the performers first.
If free disk space drops below `recorder.min_free_mb` (default 2048), proktor
logs a warning and shows it on `/health`.

### Capturing a real Traktor fixture

The committed fixture is synthetic. To replace it with a real broadcast:

```sh
./proktor -record true
# play a set with a few track changes and a deck overlap, then stop
cp recordings/<date>/<file>.ogg testdata/traktor-capture.ogg
go test -race ./...
```

The tests pick up `testdata/traktor-capture.ogg`.

## Delay and early track flips

Traktor switches its metadata as soon as the next deck starts, which can be
well before the new track is audible. During a long blend the display is
then wrong for a while. `metadata.delay_seconds` holds each detected track
back before it goes live. The default is 0. Use 30 to 60 for long blends. A
newer detection replaces the one still waiting.

The tracklist keeps both the detected time and the published time. A recording
can be cut to the time the track was actually on air, even when the display
was late.

## NixOS

The flake has a NixOS module, which is handy on a Raspberry Pi in the booth
(`aarch64-linux` is supported).

```nix
{
  inputs.proktor.url = "github:cschmittiey/proktor";

  # in a NixOS module (inputs passed through specialArgs):
  imports = [ inputs.proktor.nixosModules.default ];

  services.proktor = {
    enable = true;
    openFirewall = true;             # opens the ingest and HTTP ports
    settings = {
      event_name = "801 something";
      ingest.mount = [ { path = "/live"; on_conflict = "takeover"; } ];
      metadata.delay_seconds = 30;
      recorder.enabled = true;
      upstream = [ {
        name = "devfm";
        url = "http://radio.hack.bike:8005/";
        username = "dj_801";
        auth_style = "user_in_password";
        protocol = "put";
        hold_seconds = 20;
      } ];
    };
    # PROKTOR_HTTP_TOKEN, PROKTOR_INGEST_MOUNT_0_PASSWORD,
    # PROKTOR_UPSTREAM_0_PASSWORD, one per line.
    environmentFile = "/run/secrets/proktor.env";
  };
}
```

`settings` is rendered to TOML and ends up in the Nix store, so keep secrets
in `environmentFile`. Relative paths resolve under `/var/lib/proktor`, so
recordings go to `/var/lib/proktor/recordings`.

## Development

```sh
nix develop              # go 1.24, gopls, ffmpeg, jq, curl
go test -race ./...
testdata/gen-fixture.sh  # regenerates the synthetic fixture (needs ffmpeg)
```

Known limitations:

- The committed fixture is synthetic. A real Traktor capture still needs to be
  recorded (see above).
- Pioneer Pro DJ Link integration and audio fingerprinting are future work.
  See the Future section of [SPEC.md](SPEC.md).
