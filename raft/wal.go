package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL (Write-Ahead Log) provides durable, append-only storage for Raft state.
//
// File format per record:
//   [4 bytes] CRC32 checksum of (type + length + data)
//   [1 byte]  Record type (0=state, 1=entry)
//   [4 bytes] Data length
//   [N bytes] Data (encoded with binary.LittleEndian)
//
// State records store: currentTerm (8 bytes) + votedFor (4 bytes)
// Entry records store: Index (8) + Term (8) + CommandLen (4) + Command (N)

const (
	recordTypeState byte = 0
	recordTypeEntry byte = 1
)

// WAL is the write-ahead log.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	dir  string
	path string
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(dir string, nodeID NodeID) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("node-%d.wal", nodeID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	return &WAL{file: f, dir: dir, path: path}, nil
}

// SaveState writes currentTerm and votedFor to the WAL.
// This is called before responding to any RPC that changes term or vote.
func (w *WAL) SaveState(term uint64, votedFor NodeID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	buf := make([]byte, 12) // 8 bytes term + 4 bytes votedFor
	binary.LittleEndian.PutUint64(buf[0:8], term)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(votedFor))

	return w.writeRecord(recordTypeState, buf)
}

// SaveEntries appends log entries to the WAL.
// This is called before responding to AppendEntries or after leader appends.
func (w *WAL) SaveEntries(entries []LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		cmdLen := len(entry.Command)
		buf := make([]byte, 20+cmdLen) // 8+8+4+N
		binary.LittleEndian.PutUint64(buf[0:8], entry.Index)
		binary.LittleEndian.PutUint64(buf[8:16], entry.Term)
		binary.LittleEndian.PutUint32(buf[16:20], uint32(cmdLen))
		if cmdLen > 0 {
			copy(buf[20:], entry.Command)
		}

		if err := w.writeRecord(recordTypeEntry, buf); err != nil {
			return err
		}
	}

	// Single fsync after all entries — batched for performance
	return w.file.Sync()
}

// ReadAll reads the entire WAL and returns the latest state and all entries.
func (w *WAL) ReadAll() (term uint64, votedFor NodeID, entries []LogEntry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, 0, nil, fmt.Errorf("seek: %w", err)
	}

	for {
		recordType, data, readErr := w.readRecord()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			// Corrupted tail — truncate and continue with what we have
			break
		}

		switch recordType {
		case recordTypeState:
			if len(data) >= 12 {
				term = binary.LittleEndian.Uint64(data[0:8])
				votedFor = NodeID(binary.LittleEndian.Uint32(data[8:12]))
			}
		case recordTypeEntry:
			if len(data) >= 20 {
				idx := binary.LittleEndian.Uint64(data[0:8])
				t := binary.LittleEndian.Uint64(data[8:16])
				cmdLen := binary.LittleEndian.Uint32(data[16:20])
				var cmd []byte
				if cmdLen > 0 && len(data) >= 20+int(cmdLen) {
					cmd = make([]byte, cmdLen)
					copy(cmd, data[20:20+cmdLen])
				}
				entries = append(entries, LogEntry{Index: idx, Term: t, Command: cmd})
			}
		}
	}

	// Seek back to end for future appends
	w.file.Seek(0, io.SeekEnd)

	return term, votedFor, entries, nil
}

// Truncate replaces the WAL with a fresh file containing the given state.
// Used after snapshot installation to compact the WAL.
func (w *WAL) Truncate(term uint64, votedFor NodeID, entries []LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file
	w.file.Close()

	// Create new file (overwrites)
	f, err := os.Create(w.path)
	if err != nil {
		return fmt.Errorf("create new wal: %w", err)
	}
	w.file = f

	// Write current state
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[0:8], term)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(votedFor))
	if err := w.writeRecord(recordTypeState, buf); err != nil {
		return err
	}

	// Write all entries
	for _, entry := range entries {
		cmdLen := len(entry.Command)
		ebuf := make([]byte, 20+cmdLen)
		binary.LittleEndian.PutUint64(ebuf[0:8], entry.Index)
		binary.LittleEndian.PutUint64(ebuf[8:16], entry.Term)
		binary.LittleEndian.PutUint32(ebuf[16:20], uint32(cmdLen))
		if cmdLen > 0 {
			copy(ebuf[20:], entry.Command)
		}
		if err := w.writeRecord(recordTypeEntry, ebuf); err != nil {
			return err
		}
	}

	return w.file.Sync()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// writeRecord writes a single record with CRC checksum. Caller must hold mu.
func (w *WAL) writeRecord(recordType byte, data []byte) error {
	// Compute CRC over type + data
	crc := crc32.NewIEEE()
	crc.Write([]byte{recordType})
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	crc.Write(lenBuf)
	crc.Write(data)
	checksum := crc.Sum32()

	// Write: [4 CRC][1 type][4 length][N data]
	header := make([]byte, 9)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	header[4] = recordType
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(data)))

	if _, err := w.file.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}

// readRecord reads a single record. Caller must hold mu.
func (w *WAL) readRecord() (byte, []byte, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(w.file, header); err != nil {
		return 0, nil, err
	}

	expectedCRC := binary.LittleEndian.Uint32(header[0:4])
	recordType := header[4]
	dataLen := binary.LittleEndian.Uint32(header[5:9])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(w.file, data); err != nil {
		return 0, nil, err
	}

	// Verify CRC
	crc := crc32.NewIEEE()
	crc.Write([]byte{recordType})
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(dataLen))
	crc.Write(lenBuf)
	crc.Write(data)
	if crc.Sum32() != expectedCRC {
		return 0, nil, fmt.Errorf("crc mismatch: corrupted record")
	}

	return recordType, data, nil
}
