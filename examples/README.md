# Building a display for proktor

proktor sits between the DJ booth and the outside world and knows what is
playing. It exposes that over a small HTTP API on its own port (default
`:8080`). Displays, like a vintage CRT terminal, are separate clients built
against this API. This is the document to read if you are building one.

Reads need no authentication. Everything below is plain HTTP on the event LAN.
Every response carries `Access-Control-Allow-Origin: *`, so a browser-based
display can fetch from any origin.

| Endpoint | What |
|---|---|
| `GET /events` | Server-Sent Events. **Recommended for displays.** |
| `GET /now` | Current state as JSON. |
| `GET /now.txt` | Current state as 1-2 lines of plain text. |
| `GET /tracks` | Tonight's tracklist, JSON (`?format=txt` for plain text). |
| `GET /health` | Component status, for a status line or monitoring. |
| `POST /now` | Manual control (needs a bearer token). Not needed by displays. |
| `GET /` | Phone-friendly control page. |

## The state object

`/now`, every `/events` message, and the response to `POST /now` all use this
shape:

```json
{
  "v": 1,
  "mode": "track",
  "track": {
    "artist": "New Order",
    "title": "Blue Monday",
    "source": "traktor",
    "started_at": "2026-10-13T22:14:05Z"
  },
  "performer": "dj example",
  "event_name": "801 something",
  "history": [
    {
      "artist": "Kraftwerk",
      "title": "Computer Love",
      "source": "traktor",
      "started_at": "2026-10-13T22:09:05Z"
    }
  ],
  "pending": null,
  "seq": 42,
  "updated_at": "2026-10-13T22:14:05Z"
}
```

| Field | Meaning |
|---|---|
| `v` | Schema version, currently `1`. |
| `mode` | `"track"` (a track is playing), `"live"` (a live act, no track metadata) or `"idle"` (between acts). |
| `track` | The current track, or `null` unless `mode` is `"track"`. `artist` or `title` can be an empty string. `source` is `"traktor"`, `"manual"` or `"webhook"`. |
| `performer` | Name of the current DJ / act, or `null` if unknown. |
| `event_name` | Configured event name; may be empty. |
| `history` | Tracks that were live before the current one, newest first. Never `null`; empty array when there are none. Does not include `track`. |
| `pending` | A detected track that is not live yet (proktor holds new tracks back for a configurable delay because Traktor flips early). `null` if none. Same fields as a track plus `detected_at` and `publish_at`. **Displays should normally ignore it**; it is shown on the control page. |
| `seq` | Increments on every change. Use it to ignore duplicates. |
| `updated_at` | When the state last changed. |

### Contract rules

- **Times are UTC, RFC 3339.** Clients compute "elapsed" themselves
  (`now - track.started_at`).
- **Strings are UTF-8 exactly as received** from the DJ software. proktor does
  not transliterate. A serial terminal needs to fold to ASCII itself; see
  `tty.sh` (iconv) and `now.py` (unicodedata) for two ways.
- **Additive changes only.** New fields may appear at any time, so ignore
  fields you do not know. Renaming or removing a field would be a breaking
  change, and would come with a bump of `"v"`. If you care, check `v == 1`.
- Fields may be `null` or empty strings; do not assume an artist and title
  are always both present.

## `GET /events` (Server-Sent Events)

Content type `text/event-stream`. No polling, updates arrive instantly.

```
retry: 2000

event: state
id: 42
data: {"v":1,"mode":"track", ... }

:ping

event: state
id: 43
data: {"v":1,"mode":"idle", ... }
```

- On connect you immediately get one `state` event with the full current
  state. After that, one `state` event per change. `id` is the state's `seq`.
- `data` is the state JSON on a single line.
- A `:ping` comment line arrives every 15 seconds. If you see nothing at all
  for about 45 seconds, the connection is dead: drop it and reconnect.
- `retry: 2000` tells browser `EventSource` clients to reconnect after 2 s.
  Other clients should reconnect with a short sleep, and should show something
  like "proktor offline" while disconnected.
- You always get the full state again after reconnecting, so there is nothing
  to resume.

Minimal shell client (this is all `tty.sh` is, plus ASCII folding):

```sh
curl -sN http://proktor.lan:8080/events | while read -r line; do
  case $line in data:*) echo "${line#data: }" | jq -r '.track.artist + " - " + .track.title' ;; esac
done
```

## `GET /now.txt`

For the laziest possible client (`watch curl`, a shell loop writing to
`/dev/ttyUSB0`). `text/plain; charset=utf-8`, every line ends with `\n`.

| Mode | Output |
|---|---|
| `track` | Line 1: `Artist - Title` (the ` - ` is dropped if one side is empty). Line 2: the performer, only when known. |
| `live` | One line: `live: <performer>` (just `live` if no performer is set). |
| `idle` | One line: the event name, or `idle` if there is none. |

```
$ curl -s http://proktor.lan:8080/now.txt
New Order - Blue Monday
dj example
```

## `GET /tracks`

JSON array of tonight's tracklist (chronological, as the server logged it):

```json
[{"artist":"Kraftwerk","title":"Computer Love","source":"traktor",
  "performer":"dj example","mount":"/live",
  "detected_at":"2026-10-13T22:09:00Z","published_at":"2026-10-13T22:09:30Z"}]
```

`published_at` is `null` if the track never went live (replaced while pending,
skipped, or overridden). `?format=txt` gives one line per entry,
`HH:MM:SS  Artist - Title` (UTC detection time), with a trailing
`  (not shown)` when it was never published.

## `GET /health`

Always HTTP 200 (it is for display); look at `ok`:

```json
{
  "ok": false,
  "components": {
    "ingest":   {"ok": true, "detail": {"connected": true}},
    "recorder": {"ok": false, "warnings": ["disk low: 1.2 GiB free"]}
  },
  "time": "2026-10-13T22:15:00Z"
}
```

`ok` is true only when every component is ok. `warnings` is a list of human
readable strings; `detail` is component-specific and may change.

## `POST /now` (control)

Not needed by displays, but handy for scripts. Requires
`Authorization: Bearer <token>`.

```sh
curl -X POST http://proktor.lan:8080/now \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"action":"set","artist":"New Order","title":"Blue Monday"}'
```

Actions: `set` (artist, title, optional `source` of `manual`/`webhook`),
`performer` (performer), `live` (performer; clears the track), `clear`,
`skip` (drops the pending track), `idle`. If `action` is omitted but
`artist`/`title` are present, `set` is assumed. The response is the new state.
Errors are `{"error": "..."}`: 400 bad request, 401 missing/wrong token, 403
writes disabled (no token configured on the server).

## Reference clients

These are examples, not supported components. Copy and bend them.

- `tty.sh` - bash + curl + jq. Follows `/events`, folds to ASCII with `iconv`,
  and writes a NOW PLAYING screen to a serial device (default
  `/dev/ttyUSB0`, 9600 8N1; `-d -` for stdout). Reconnects forever and shows
  "proktor offline" while down.

  ```sh
  PROKTOR_URL=http://proktor.lan:8080 ./tty.sh -d /dev/ttyUSB0 -b 9600
  ./tty.sh -d -      # try it in your terminal first
  ```

  Tweakables: `PROKTOR_URL`, `TTY`, `BAUD`, `COLS`, `CLEAR_SEQ` (default
  `ESC[2J ESC[H`; use `$'\f'` for terminals that clear on form feed), `RETRY`.
- `now.py` - the same thing in stdlib-only Python (`unicodedata` NFKD for the
  ASCII fold).
