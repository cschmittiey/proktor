#!/usr/bin/env python3
"""Reference proktor display client in plain Python (stdlib only).

Follows GET /events and prints / writes a NOW PLAYING block, folded to ASCII
with unicodedata NFKD. Same idea as tty.sh. Example, not a supported component.

    now.py [--url http://localhost:8080] [--dev /dev/ttyUSB0 | -] [--cols 80]
"""
import argparse
import json
import sys
import time
import unicodedata
import urllib.request

CLEAR = "\x1b[2J\x1b[H"


def ascii_fold(s):
    s = unicodedata.normalize("NFKD", s)
    return "".join(c if " " <= c <= "~" else "" for c in s if not unicodedata.combining(c))


def block(st):
    lines = ["NOW PLAYING", "-----------", ""]
    track, perf = st.get("track"), st.get("performer")
    if st.get("mode") == "track" and track:
        lines.append(" - ".join(x for x in (track.get("artist"), track.get("title")) if x))
        if perf:
            lines.append(perf)
    elif st.get("mode") == "live":
        lines += ["LIVE", perf or ""]
    else:
        lines.append("(nothing playing)")
    if st.get("event_name"):
        lines += ["", st["event_name"]]
    return lines


def draw(out, lines, cols):
    wrapped = []
    for ln in lines:
        ln = ascii_fold(ln)
        wrapped += [ln[i:i + cols] for i in range(0, len(ln), cols)] or [""]
    out.write(CLEAR + "".join(w + "\r\n" for w in wrapped))
    out.flush()


def events(url):
    # timeout > 15s ping interval: a silent connection is dead.
    with urllib.request.urlopen(url + "/events", timeout=45) as resp:
        event, data = "", []
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\r\n")
            if line == "":
                if data and event in ("", "state"):
                    yield json.loads("\n".join(data))
                event, data = "", []
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].removeprefix(" "))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--dev", default="-", help="serial device or - for stdout")
    ap.add_argument("--cols", type=int, default=80)
    a = ap.parse_args()
    out = sys.stdout if a.dev == "-" else open(a.dev, "w", newline="")
    while True:
        try:
            for st in events(a.url.rstrip("/")):
                draw(out, block(st), a.cols)
        except (OSError, ValueError):
            pass
        draw(out, ["proktor offline", "", "retrying..."], a.cols)
        time.sleep(3)


if __name__ == "__main__":
    main()
