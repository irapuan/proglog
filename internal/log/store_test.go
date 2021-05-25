package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(test *testing.T) {
	tempFile, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(test, err)
	defer os.Remove(tempFile.Name())

	store, err := newStore(tempFile)
	require.NoError(test, err)

	testAppend(test, store)
	testRead(test, store)
	testReadAt(test, store)

	store, err = newStore(tempFile)
	require.NoError(test, err)
	testRead(test, store)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)

		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := encoding.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(test *testing.T) {
	tempFile, err := ioutil.TempFile("", "store_close_test")
	require.NoError(test, err)

	defer os.Remove(tempFile.Name())

	store, err := newStore(tempFile)

	require.NoError(test, err)

	_, _, err = store.Append(write)
	require.NoError(test, err)

	tempFile, beforeSize, err := openFile(tempFile.Name())
	require.NoError(test, err)

	err = store.Close()
	require.NoError(test, err)

	_, afterSize, err := openFile(tempFile.Name())

	require.NoError(test, err)
	require.True(test, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
