package relay

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/cschmittiey/proktor/internal/ogg"
)

// behavior tells the fake server how to treat one accepted connection.
type behavior struct {
	status    string // response status line after the request, e.g. "HTTP/1.1 100 Continue"; "" = 100 Continue
	dropAfter int    // close the connection once this many body bytes were read (0 = never)
	noRead    bool   // never read the body (blocks the sender)
}

type fakeConn struct {
	reqLine string
	header  textproto.MIMEHeader

	mu     sync.Mutex
	body   bytes.Buffer
	closed bool
	conn   net.Conn
}

func (c *fakeConn) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.body.Bytes()...)
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type fakeServer struct {
	t  *testing.T
	ln net.Listener

	mu      sync.Mutex
	conns   []*fakeConn
	next    func(i int) behavior
	stopped bool
	done    chan struct{}
	wg      sync.WaitGroup
}

// newFakeServer starts a fake Icecast on 127.0.0.1:0. next picks the
// behavior of the i-th connection (nil = always 100 Continue, read forever).
func newFakeServer(t *testing.T, next func(i int) behavior) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return serveFake(t, ln, next)
}

func serveFake(t *testing.T, ln net.Listener, next func(i int) behavior) *fakeServer {
	s := &fakeServer{t: t, ln: ln, next: next, done: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go s.handle(c)
		}
	}()
	t.Cleanup(s.stop)
	return s
}

func (s *fakeServer) stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.done)
	conns := append([]*fakeConn(nil), s.conns...)
	s.mu.Unlock()
	s.ln.Close()
	for _, c := range conns {
		c.conn.Close()
	}
	s.wg.Wait()
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }
func (s *fakeServer) url() string  { return "http://" + s.addr() + "/" }

func (s *fakeServer) handle(c net.Conn) {
	defer s.wg.Done()
	defer c.Close()
	br := bufio.NewReader(c)
	tp := textproto.NewReader(br)
	line, err := tp.ReadLine()
	if err != nil {
		return
	}
	hdr, err := tp.ReadMIMEHeader()
	if err != nil {
		return
	}
	fc := &fakeConn{reqLine: line, header: hdr, conn: c}
	s.mu.Lock()
	idx := len(s.conns)
	b := behavior{}
	if s.next != nil {
		b = s.next(idx)
	}
	// Registered before replying so tests that see the conn know the
	// request was complete.
	s.conns = append(s.conns, fc)
	s.mu.Unlock()
	if b.noRead {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetReadBuffer(2048)
		}
	}
	status := b.status
	if status == "" {
		status = "HTTP/1.1 100 Continue"
	}
	io.WriteString(c, status+"\r\n\r\n")
	defer func() {
		fc.mu.Lock()
		fc.closed = true
		fc.mu.Unlock()
	}()
	if !strings.Contains(status, " 100 ") && !strings.Contains(status, " 200 ") {
		return
	}
	if b.noRead {
		<-s.done // hold the connection open without ever reading
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := br.Read(buf)
		if n > 0 {
			fc.mu.Lock()
			fc.body.Write(buf[:n])
			total := fc.body.Len()
			fc.mu.Unlock()
			if b.dropAfter > 0 && total >= b.dropAfter {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *fakeServer) numConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *fakeServer) conn(i int) *fakeConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[i]
}

// ------------------------------------------------------------ ogg helpers

// mkPage builds a syntactically valid Ogg page (the CRC is not checked by
// the relay and left zero) as the scanner would emit it.
func mkPage(serial, seq uint32, bos, header bool, size int, fill byte) ogg.Segment {
	var segTable []byte
	rem := size
	for rem >= 255 {
		segTable = append(segTable, 255)
		rem -= 255
	}
	segTable = append(segTable, byte(rem))
	h := make([]byte, 27)
	copy(h, "OggS")
	if bos {
		h[5] |= 0x02
	}
	binary.LittleEndian.PutUint32(h[14:], serial)
	binary.LittleEndian.PutUint32(h[18:], seq)
	h[26] = byte(len(segTable))
	data := append(h, segTable...)
	data = append(data, bytes.Repeat([]byte{fill}, size)...)
	return ogg.Segment{Data: data, Page: true, BOS: bos, Header: header || bos, Serial: serial, Sequence: seq}
}

func garbage(n int) ogg.Segment { return ogg.Segment{Data: bytes.Repeat([]byte{0xAA}, n)} }

type pageInfo struct {
	serial, seq uint32
	bos         bool
	fill        byte
	raw         []byte
}

// splitPages parses data as consecutive whole Ogg pages; it fails the test on
// anything else (mid-page starts, garbage, truncation).
func splitPages(t *testing.T, data []byte) []pageInfo {
	t.Helper()
	var out []pageInfo
	for len(data) > 0 {
		if len(data) < 27 || string(data[:4]) != "OggS" {
			t.Fatalf("not at a page boundary: %d bytes left, starts %x", len(data), data[:min(len(data), 8)])
		}
		n := int(data[26])
		if len(data) < 27+n {
			t.Fatalf("truncated segment table")
		}
		size := 27 + n
		for _, b := range data[27 : 27+n] {
			size += int(b)
		}
		if len(data) < size {
			t.Fatalf("truncated page: need %d have %d", size, len(data))
		}
		p := pageInfo{
			serial: binary.LittleEndian.Uint32(data[14:]),
			seq:    binary.LittleEndian.Uint32(data[18:]),
			bos:    data[5]&0x02 != 0,
			raw:    data[:size],
		}
		if size > 27+n {
			p.fill = data[27+n]
		}
		out = append(out, p)
		data = data[size:]
	}
	return out
}

func concat(segs ...ogg.Segment) []byte {
	var b []byte
	for _, s := range segs {
		b = append(b, s.Data...)
	}
	return b
}
