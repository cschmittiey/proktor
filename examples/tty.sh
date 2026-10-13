#!/usr/bin/env bash
# tty.sh - reference proktor display client for a serial CRT terminal.
#
# Subscribes to proktor's /events (Server-Sent Events) and, on every state
# change, clears the terminal and writes a NOW PLAYING block, folded to ASCII.
# This is an example, not a supported component: copy it and bend it to your
# terminal.
#
# Usage:  tty.sh [-u URL] [-d DEVICE] [-b BAUD] [-w COLS]
#   -u URL     proktor base URL         (env PROKTOR_URL,  default http://localhost:8080)
#   -d DEVICE  serial device, or "-" for stdout
#                                       (env TTY,          default /dev/ttyUSB0)
#   -b BAUD    baud rate                (env BAUD,         default 9600)
#   -w COLS    screen width to wrap at  (env COLS,         default 80)
# Other env:
#   CLEAR_SEQ  clear-screen sequence    (default ESC[2J ESC[H, i.e. ANSI/VT100)
#   RETRY      seconds between reconnects (default 3)
#   STALE      seconds of silence before the connection is considered dead
#              (default 45; proktor pings every 15s)
#
# Needs: bash, curl, jq. Uses iconv for UTF-8 -> ASCII when it can, with a
# plain strip-non-ASCII fallback. Line endings are written as CR LF.

set -u

URL=${PROKTOR_URL:-http://localhost:8080}
TTY=${TTY:-/dev/ttyUSB0}
BAUD=${BAUD:-9600}
COLS=${COLS:-80}
RETRY=${RETRY:-3}
STALE=${STALE:-45}
CLEAR_SEQ=${CLEAR_SEQ-$'\033[2J\033[H'}

while getopts "u:d:b:w:h" opt; do
  case $opt in
    u) URL=$OPTARG ;;
    d) TTY=$OPTARG ;;
    b) BAUD=$OPTARG ;;
    w) COLS=$OPTARG ;;
    *) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
URL=${URL%/}

for cmd in curl jq; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "tty.sh: $cmd is required" >&2; exit 1; }
done

# --- output device ---------------------------------------------------------
if [ "$TTY" = "-" ]; then
  exec 3>&1
else
  if [ -c "$TTY" ]; then
    # 8N1, raw-ish: no echo, no flow control, no output post-processing
    # (we send CR LF ourselves).
    stty -F "$TTY" "$BAUD" cs8 -parenb -cstopb -ixon -ixoff -crtscts -echo -opost \
      || echo "tty.sh: warning: stty failed on $TTY" >&2
  fi
  exec 3>"$TTY" || { echo "tty.sh: cannot open $TTY" >&2; exit 1; }
fi

# --- UTF-8 -> ASCII --------------------------------------------------------
# iconv's //TRANSLIT only transliterates (e-acute -> e) in a UTF-8 locale, so
# probe for one that works. If none does, just strip non-ASCII bytes.
FOLD_LOCALE=""
if command -v iconv >/dev/null 2>&1; then
  for loc in "" C.UTF-8 C.utf8 en_US.UTF-8 en_US.utf8; do
    out=$(printf '\303\251' | LC_ALL=${loc:-${LC_ALL:-${LANG:-C}}} iconv -f UTF-8 -t ASCII//TRANSLIT 2>/dev/null)
    if [ "$out" = "e" ]; then FOLD_LOCALE=${loc:-${LC_ALL:-${LANG:-C}}}; break; fi
  done
fi

to_ascii() {
  if [ -n "$FOLD_LOCALE" ]; then
    LC_ALL=$FOLD_LOCALE iconv -f UTF-8 -t ASCII//TRANSLIT 2>/dev/null
  else
    LC_ALL=C sed 's/[^[:print:]	]//g'
  fi | LC_ALL=C tr -cd '\11\12\40-\176'
}

# --- drawing ---------------------------------------------------------------
# draw TEXT: fold to ASCII, wrap, CR LF, clear screen first.
draw() {
  {
    printf '%s' "$CLEAR_SEQ"
    printf '%s\n' "$1" | to_ascii | fold -s -w "$COLS" | sed 's/$/\r/'
  } >&3
}

render() {
  local text
  text=$(printf '%s' "$1" | jq -r '
    def at: if (.artist // "") != "" and (.title // "") != "" then .artist + " - " + .title
            else (.artist // "") + (.title // "") end;
    ["NOW PLAYING", "-----------", ""]
    + (if .mode == "track" and .track != null then
         [ (.track | at) ] + (if (.performer // "") != "" then [.performer] else [] end)
       elif .mode == "live" then
         ["LIVE", (.performer // "")]
       else
         ["(nothing playing)"]
       end)
    + (if (.event_name // "") != "" then ["", .event_name] else [] end)
    | join("\n")' 2>/dev/null) || return 0
  [ -n "$text" ] && draw "$text"
}

OFFLINE_SHOWN=0
show_offline() {
  [ "$OFFLINE_SHOWN" = 1 ] && return
  OFFLINE_SHOWN=1
  draw $'proktor offline\n\nretrying...'
}

# --- main loop -------------------------------------------------------------
# One SSE connection. Returns when the stream ends, errors, or goes silent.
stream() {
  local line event="" data="" curl_pid
  exec 4< <(curl -sSN --fail --connect-timeout 5 \
              -H 'Accept: text/event-stream' "$URL/events" 2>/dev/null)
  curl_pid=$!
  while IFS= read -r -t "$STALE" line <&4; do
    line=${line%$'\r'}
    case $line in
      "")        # blank line ends an event
        if [ -n "$data" ] && { [ -z "$event" ] || [ "$event" = state ]; }; then
          OFFLINE_SHOWN=0
          render "$data"
        fi
        event=""; data="" ;;
      event:*)   event=${line#event:}; event=${event# } ;;
      data:*)    d=${line#data:}; data=${data:+$data$'\n'}${d# } ;;
    esac
  done
  kill "$curl_pid" 2>/dev/null
  exec 4<&-
}

trap 'exit 0' INT TERM
while :; do
  stream
  show_offline
  sleep "$RETRY"
done
