package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

// Journal format: JSON lines, append-only, one record per line.
//
//	{"type":"state","state":{...State...},"manual_performer":"x","auto_performer":"y"}
//	{"type":"tracklist","i":3,"entry":{...TracklistEntry...}}
//
// A "state" record is written on every visible state change (and when only
// the internal manual/auto performers changed); the last one wins on replay.
// A "tracklist" record sets entry i (appending when i == len, replacing
// otherwise), so an entry being published later is just a second record for
// the same index. Unknown record types and unparseable lines (including a
// truncated final line from a crash) are skipped.
type record struct {
	Type            string          `json:"type"`
	State           *State          `json:"state,omitempty"`
	ManualPerformer string          `json:"manual_performer,omitempty"`
	AutoPerformer   string          `json:"auto_performer,omitempty"`
	I               *int            `json:"i,omitempty"`
	Entry           *TracklistEntry `json:"entry,omitempty"`
}

type journal struct {
	f   *os.File
	log *slog.Logger
}

type replayResult struct {
	state        *State
	manual, auto string
	tracklist    []TracklistEntry
	needsNewline bool // file does not end in '\n' (truncated last line)
}

func replayJournal(path string, log *slog.Logger) (replayResult, error) {
	var rs replayResult
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return rs, nil
	}
	if err != nil {
		return rs, fmt.Errorf("state: read journal: %w", err)
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		rs.needsNewline = true
	}
	for n, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(line, &rec); err != nil {
			log.Warn("state: skipping unreadable journal line", "line", n+1, "err", err)
			continue
		}
		switch rec.Type {
		case "state":
			if rec.State == nil {
				continue
			}
			s := *rec.State
			rs.state = &s
			rs.manual, rs.auto = rec.ManualPerformer, rec.AutoPerformer
		case "tracklist":
			if rec.I == nil || rec.Entry == nil || *rec.I < 0 {
				continue
			}
			e := *rec.Entry
			e.DetectedAt = e.DetectedAt.UTC()
			if e.PublishedAt != nil {
				p := e.PublishedAt.UTC()
				e.PublishedAt = &p
			}
			if i := *rec.I; i < len(rs.tracklist) {
				rs.tracklist[i] = e
			} else {
				rs.tracklist = append(rs.tracklist, e)
			}
		default:
			// unknown type: forward compatible
		}
	}
	if rs.state != nil {
		s := *rs.state
		s.UpdatedAt = s.UpdatedAt.UTC()
		if s.Track != nil {
			s.Track.StartedAt = s.Track.StartedAt.UTC()
		}
		for i := range s.History {
			s.History[i].StartedAt = s.History[i].StartedAt.UTC()
		}
		if s.Pending != nil {
			s.Pending.StartedAt = s.Pending.StartedAt.UTC()
			s.Pending.DetectedAt = s.Pending.DetectedAt.UTC()
			s.Pending.PublishAt = s.Pending.PublishAt.UTC()
		}
		rs.state = &s
	}
	return rs, nil
}

func openJournal(path string, log *slog.Logger, needsNewline bool) (*journal, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("state: open journal: %w", err)
	}
	if needsNewline {
		// Terminate a truncated last line so new records start on their own.
		if _, err := f.Write([]byte{'\n'}); err != nil {
			f.Close()
			return nil, fmt.Errorf("state: repair journal: %w", err)
		}
	}
	return &journal{f: f, log: log}, nil
}

// All write helpers are nil-safe so persistence can be disabled.

func (j *journal) write(rec record) {
	if j == nil {
		return
	}
	b, err := json.Marshal(rec)
	if err != nil {
		j.log.Error("state: journal marshal", "err", err)
		return
	}
	b = append(b, '\n')
	if _, err := j.f.Write(b); err != nil { // one Write per line, O_APPEND
		j.log.Error("state: journal write", "err", err)
	}
}

func (j *journal) writeState(s State, manual, auto string) {
	if j == nil {
		return
	}
	j.write(record{Type: "state", State: &s, ManualPerformer: manual, AutoPerformer: auto})
}

func (j *journal) writeEntry(i int, e TracklistEntry) {
	if j == nil {
		return
	}
	j.write(record{Type: "tracklist", I: &i, Entry: &e})
}

func (j *journal) close() error {
	if j == nil {
		return nil
	}
	return j.f.Close()
}
