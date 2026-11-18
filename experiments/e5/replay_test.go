// E5: does net/http.Transport replay a POST on a reused keep-alive connection
// when req.GetBody is nil? A raw TCP server closes the connection on /drop
// without answering; we count how many times /drop reaches the server.
//
// Run: go test -race -count=1 -v ./...
package e5

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"testing"
	"time"
)

type server struct {
	ln     net.Listener
	mu     sync.Mutex
	counts map[string]int
}

func startServer(t *testing.T) *server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{ln: ln, counts: map[string]int{}}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *server) serve(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	for {
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, req.Body)
		req.Body.Close()
		s.mu.Lock()
		s.counts[req.URL.Path]++
		s.mu.Unlock()
		if req.URL.Path == "/drop" {
			return // close without writing anything
		}
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
	}
}

func (s *server) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[path]
}

func TestReplay(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		nilGetBody bool
		want       int
	}{
		{"control_GetBody_set_with_Idempotency-Key", "Idempotency-Key", false, 2},
		{"GetBody_nil_with_Idempotency-Key", "Idempotency-Key", true, 1},
		{"control_no_Idempotency-Key_GetBody_set", "", false, 1},
		{"GetBody_nil_with_X-Idempotency-Key", "X-Idempotency-Key", true, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := startServer(t)
			base := "http://" + s.ln.Addr().String()
			tr := &http.Transport{}
			t.Cleanup(tr.CloseIdleConnections)
			client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

			// a. warm the pool
			resp, err := client.Post(base+"/ok", "application/json", bytes.NewReader([]byte(`{"a":1}`)))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			// b. POST /drop on the pooled connection
			req, err := http.NewRequest(http.MethodPost, base+"/drop", bytes.NewReader([]byte(`{"b":2}`)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				req.Header.Set(tc.header, "00000000-0000-4000-8000-000000000001")
			}
			if tc.nilGetBody {
				req.GetBody = nil
			}
			reused, seen := false, false
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
				GotConn: func(i httptrace.GotConnInfo) {
					if !seen { // a replay triggers a second GotConn on a new conn
						seen, reused = true, i.Reused
					}
				},
			}))
			resp, err = client.Do(req)
			if err == nil {
				resp.Body.Close()
				t.Fatal("expected an error from client.Do")
			}
			t.Logf("client.Do error: %v (type %T)", err, err)

			// c. assertions
			if !reused {
				t.Fatal("first GotConn was not a reused connection; test proves nothing")
			}
			if got := s.count("/drop"); got != tc.want {
				t.Errorf("/drop received %d times, want %d", got, tc.want)
			} else {
				t.Logf("/drop received %d times", got)
			}
		})
	}
}
