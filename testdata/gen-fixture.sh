#!/usr/bin/env bash
# Generates testdata/chained.ogg: a SYNTHETIC chained Ogg Vorbis stream that
# imitates what Traktor's broadcast produces (one logical bitstream per track,
# each with ARTIST/TITLE Vorbis comments, concatenated back to back).
#
# Requires ffmpeg built with libvorbis. Output is deterministic for a given
# ffmpeg/libvorbis version: bitexact flags, fixed stream serial numbers.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

out=chained.ogg
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# freq|artist|title
tracks=(
  "220|New Order|Blue Monday"
  "277|Röyksopp|Eple"
  "330|坂本龍一|Merry Christmas Mr. Lawrence"
  "392|Aphex Twin|Xtal"
  "494|DJ Überlong Name feat. Ünïcödé|Track with = equals sign"
)

n=0
for t in "${tracks[@]}"; do
  IFS='|' read -r freq artist title <<<"$t"
  n=$((n + 1))
  ffmpeg -nostdin -hide_banner -loglevel error -y \
    -f lavfi -i "sine=frequency=${freq}:sample_rate=44100:duration=3" \
    -ac 2 -c:a libvorbis -q:a 0 \
    -fflags +bitexact -flags:a +bitexact \
    -map_metadata -1 \
    -metadata "ARTIST=${artist}" -metadata "TITLE=${title}" \
    -serial_offset $((n * 1000)) \
    -f ogg "$tmp/$n.ogg"
done

# Chaining is plain concatenation.
cat "$tmp"/[0-9].ogg >"$out"
ls -l "$out"
