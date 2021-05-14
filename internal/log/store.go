package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

/*
We refer to the enc variable and lenWidth constant repeatedly in the store,
so we place them up top where they’re easy to find. enc defines the encoding that we
persist record sizes and index entries in and lenWidth defines the number of bytes used to
store the record’s length.
*/
var encoding = binary.BigEndian

const lenWidth = 8

/*
The store struct is a simple wrapper around a file with two APIs to append and read
bytes to and from the file.
*/
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

/*
The newStore(*os.File) function creates a store for the
given file. The function calls os.Stat(name string) to get the file’s current size,
in case we’re re-creating the store from a file that has existing data, which would happen if,
for example, our service had restarted.


os.Stat returns a Fileinfo struct that looks like:
type FileInfo interface {
    Name() string       // base name of the file
    Size() int64        // length in bytes for regular files; system-dependent for others
    Mode() FileMode     // file mode bits
    ModTime() time.Time // modification time
    IsDir() bool        // abbreviation for Mode().IsDir()
    Sys() interface{}   // underlying data source (can return nil)
}
*/
func newStore(inputFile *os.File) (*store, error) {
	fi, err := os.Stat(inputFile.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: inputFile,
		size: size,
		buf:  bufio.NewWriter(inputFile), // Use bufio to not do syscalls all the time we want to write to the file.
	}, nil
}

/*
Append([]byte) persists the given bytes to the store. We write the length of the record so that,
when we read the record, we know how many bytes to read.
We write to the buffered writer instead of directly to the file to reduce the
number of system calls and improve performance. If a user wrote a lot of small records,
this would help a lot. Then we return the number of bytes written, which similar Go APIs
conventionally do, and the position where the store holds the record in its file. The segment will
use this position when it creates an associated index entry for this record.
*/
func (s *store) Append(input []byte) (bytesWritten uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, encoding, uint64(len(input))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(input)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil

}

/*
Read(pos uint64) returns the record stored at the given position.
First it flushes the writer buffer, in case we’re about to try to read a
record that the buffer hasn’t flushed to disk yet.
We find out how many bytes we have to read to get the whole record, and then we
fetch and return the record. The compiler allocates byte slices that don’t escape
the functions they’re declared in on the stack. A value escapes when it lives beyond
the lifetime of the function call—if you return the value, for example.
*/
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, encoding.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

/*
ReadAt(p []byte, off int64) reads len(p) bytes into p beginning at the off offset in
the store’s file. It implements io.ReaderAt on the store type.
*/
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
