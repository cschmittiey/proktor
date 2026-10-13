package ingest

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"
)

const (
	maxHeadBytes   = 64 << 10
	maxHeaderLines = 200
	writeTimeout   = 10 * time.Second
)

var errHeadTooLarge = errors.New("request head too large")

// connReader feeds bufio from the connection. During the request head it
// enforces a byte limit; afterwards it arms a fresh idle deadline before
// every read so that a silent source is dropped.
type connReader struct {
	conn      net.Conn
	remaining int           // head budget; <=0 with idle>0 means unlimited
	idle      time.Duration // 0 while reading the head
}

func (r *connReader) Read(p []byte) (int, error) {
	if r.idle > 0 {
		r.conn.SetReadDeadline(time.Now().Add(r.idle))
		return r.conn.Read(p)
	}
	if r.remaining <= 0 {
		return 0, errHeadTooLarge
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.conn.Read(p)
	r.remaining -= n
	return n, err
}

// request is a parsed source request head.
type request struct {
	method  string
	target  string
	proto   string
	mount   string
	headers map[string]string // lower-cased names
}

func (r *request) header(names ...string) string {
	for _, n := range names {
		if v := r.headers[n]; v != "" {
			return v
		}
	}
	return ""
}

// readRequest parses the request line and headers leniently (source clients
// are not always strict HTTP).
func readRequest(br *bufio.Reader) (*request, error) {
	tp := textproto.NewReader(br)
	line, err := tp.ReadLine()
	if err != nil {
		return nil, err
	}
	f := strings.Fields(line)
	if len(f) < 2 || len(f) > 3 {
		return nil, fmt.Errorf("malformed request line %q", truncate(line, 80))
	}
	req := &request{method: strings.ToUpper(f[0]), target: f[1], headers: map[string]string{}}
	if len(f) == 3 {
		req.proto = f[2]
	}
	for i := 0; ; i++ {
		if i > maxHeaderLines {
			return nil, errHeadTooLarge
		}
		l, err := tp.ReadLine()
		if err != nil {
			return nil, err
		}
		if l == "" {
			break
		}
		if l[0] == ' ' || l[0] == '\t' {
			continue // obsolete line folding: ignore
		}
		k, v, ok := strings.Cut(l, ":")
		if !ok {
			continue
		}
		k = strings.ToLower(strings.TrimSpace(k))
		if _, dup := req.headers[k]; !dup {
			req.headers[k] = strings.TrimSpace(v)
		}
	}
	m := req.target
	if i := strings.IndexAny(m, "?#"); i >= 0 {
		m = m[:i]
	}
	if m == "" || m[0] != '/' {
		m = "/" + m
	}
	req.mount = m
	return req, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// basicPassword extracts the password from an "Authorization: Basic" header.
func basicPassword(h string) (string, bool) {
	scheme, rest, ok := strings.Cut(strings.TrimSpace(h), " ")
	if !ok || !strings.EqualFold(scheme, "basic") {
		return "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rest))
	if err != nil {
		return "", false
	}
	_, pw, ok := strings.Cut(string(raw), ":")
	return pw, ok
}

func passwordOK(want, got string) bool {
	a := sha256.Sum256([]byte(want))
	b := sha256.Sum256([]byte(got))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func writeResponse(conn net.Conn, code int, status string, extra []string, body string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "HTTP/1.0 %d %s\r\n", code, status)
	sb.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	fmt.Fprintf(&sb, "Content-Length: %d\r\n", len(body))
	for _, e := range extra {
		sb.WriteString(e)
		sb.WriteString("\r\n")
	}
	sb.WriteString("Connection: close\r\n\r\n")
	sb.WriteString(body)
	return writeRaw(conn, sb.String())
}

func writeRaw(conn net.Conn, s string) error {
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := io.WriteString(conn, s)
	conn.SetWriteDeadline(time.Time{})
	return err
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "000000"[:2*n]
	}
	return hex.EncodeToString(b)
}
