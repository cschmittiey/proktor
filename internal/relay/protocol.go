package relay

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/cschmittiey/proktor/internal/stream"
)

// connect dials the upstream and performs the source handshake. On success
// the connection is ready to receive the stream body; br holds any bytes the
// server sent after the response headers. The connection is closed when ctx
// is cancelled (and on error).
func (u *upstream) connect(ctx context.Context, info stream.SessionInfo) (conn net.Conn, br *bufio.Reader, err error) {
	opts := u.r.opts
	hsCtx, cancel := context.WithTimeout(ctx, opts.HandshakeTimeout)
	defer cancel()

	raw, err := opts.Dialer.DialContext(hsCtx, "tcp", u.host)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", u.host, err)
	}
	conn = raw
	context.AfterFunc(ctx, func() { raw.Close() })
	defer func() {
		if err != nil {
			raw.Close()
			conn, br = nil, nil
		}
	}()

	if u.useTLS {
		cfg := opts.TLSConfig.Clone()
		if cfg == nil {
			cfg = &tls.Config{}
		}
		if cfg.ServerName == "" {
			cfg.ServerName = u.hostname
		}
		tc := tls.Client(raw, cfg)
		if err = tc.HandshakeContext(hsCtx); err != nil {
			return nil, nil, fmt.Errorf("tls handshake with %s: %w", u.host, err)
		}
		conn = tc
	}

	conn.SetDeadline(time.Now().Add(opts.HandshakeTimeout))
	if _, err = conn.Write(u.buildRequest(info)); err != nil {
		return nil, nil, fmt.Errorf("send request: %w", err)
	}
	br = bufio.NewReader(conn)
	tp := textproto.NewReader(br)
	line, err := tp.ReadLine()
	if err != nil {
		return nil, nil, fmt.Errorf("read response: %w", err)
	}
	if _, err = tp.ReadMIMEHeader(); err != nil {
		return nil, nil, fmt.Errorf("read response headers: %w", err)
	}
	parts := strings.SplitN(line, " ", 3)
	code := 0
	if len(parts) >= 2 && strings.HasPrefix(parts[0], "HTTP/") {
		code, _ = strconv.Atoi(parts[1])
	}
	switch {
	case code == 100 || (code >= 200 && code < 300):
	case code == 0:
		return nil, nil, fmt.Errorf("unexpected response %q", line)
	default:
		return nil, nil, fmt.Errorf("upstream rejected the stream: %s", strings.TrimSpace(strings.TrimPrefix(line, parts[0])))
	}
	conn.SetDeadline(time.Time{})
	return conn, br, nil
}

// buildRequest renders the source request line and headers.
func (u *upstream) buildRequest(info stream.SessionInfo) []byte {
	c := u.cfg
	var sb strings.Builder
	if c.Protocol == "source" {
		fmt.Fprintf(&sb, "SOURCE %s HTTP/1.0\r\n", c.Mount)
	} else {
		fmt.Fprintf(&sb, "PUT %s HTTP/1.1\r\n", c.Mount)
	}
	cred := c.Username + ":" + c.Password
	if c.AuthStyle == "user_in_password" {
		cred = "source:" + c.Username + ":" + c.Password
	}
	hdr := func(k, v string) { fmt.Fprintf(&sb, "%s: %s\r\n", k, cleanHeader(v)) }
	hdr("Host", u.host)
	hdr("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(cred)))
	hdr("Content-Type", c.ContentType)
	hdr("User-Agent", u.r.opts.UserAgent)
	hdr("ice-public", "0")
	for _, kv := range [][2]string{
		{"ice-name", info.IceName},
		{"ice-description", info.IceDescription},
		{"ice-genre", info.IceGenre},
		{"ice-url", info.IceURL},
	} {
		if kv[1] != "" {
			hdr(kv[0], kv[1])
		}
	}
	if c.Protocol != "source" {
		hdr("Expect", "100-continue")
	}
	sb.WriteString("\r\n")
	return []byte(sb.String())
}

// cleanHeader strips CR/LF so source-supplied values (e.g. a stream name)
// cannot inject headers into the upstream request.
func cleanHeader(v string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(v)
}
