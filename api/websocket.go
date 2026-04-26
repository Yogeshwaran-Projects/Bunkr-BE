package api

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

// wsConn is a simple WebSocket connection (RFC 6455).
type wsConn struct {
	conn   net.Conn
	mu     sync.Mutex
	closed bool
}

// handleWebSocket upgrades HTTP to WebSocket and registers the client.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	ws := &wsConn{conn: conn}

	s.wsMu.Lock()
	s.wsClients[ws] = true
	s.wsMu.Unlock()

	log.Printf("WebSocket client connected (%d total)", len(s.wsClients))

	// Read loop (just to detect disconnect)
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			break
		}
	}

	s.wsMu.Lock()
	delete(s.wsClients, ws)
	s.wsMu.Unlock()

	ws.close()
	log.Printf("WebSocket client disconnected (%d total)", len(s.wsClients))
}

// send writes a text frame to the WebSocket connection.
func (ws *wsConn) send(data []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.closed {
		return fmt.Errorf("connection closed")
	}

	return writeWSFrame(ws.conn, data)
}

func (ws *wsConn) close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if !ws.closed {
		ws.closed = true
		ws.conn.Close()
	}
}

// upgradeWebSocket performs the WebSocket handshake.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if r.Header.Get("Upgrade") != "websocket" {
		return nil, fmt.Errorf("not a websocket request")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Key")
	}

	// Compute accept key per RFC 6455
	magic := "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + magic))
	acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Hijack the connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("server doesn't support hijacking")
	}

	conn, bufrw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	// Write upgrade response
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := bufrw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

// writeWSFrame writes a WebSocket text frame (opcode 0x1).
func writeWSFrame(w io.Writer, payload []byte) error {
	bw := bufio.NewWriter(w)

	// FIN + text opcode
	bw.WriteByte(0x81)

	// Payload length (server → client: no masking)
	length := len(payload)
	if length < 126 {
		bw.WriteByte(byte(length))
	} else if length < 65536 {
		bw.WriteByte(126)
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, uint16(length))
		bw.Write(lenBytes)
	} else {
		bw.WriteByte(127)
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(length))
		bw.Write(lenBytes)
	}

	bw.Write(payload)
	return bw.Flush()
}
