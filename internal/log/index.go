package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

/*
The (off|pos|ent)Width constants below define the number of bytes
that make up each index entry
*/
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

/*
index defines our index file, which comprises a persisted file
and a memory-mapped file (mmap).
The size tells us the size of the index and where to write
the next entry appended to the index.
*/
type index struct {
	file *os.File
	mmap gommap.MMap // https://medium.com/i0exception/memory-mapped-files-5e083e653b1
	size uint64
}

/*
newIndex(*os.File) creates an index for the given file.
We create the index and save the current size of the
file so we can track the amount of data in the index file
as we add index entries. We grow the file to the max index
size before memory-mapping the file and then return the created index to the caller.
*/
func newIndex(file *os.File, config Config) (*index, error) {
	idx := &index{
		file: file,
	}
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	if err = os.Truncate(
		file.Name(), int64(config.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil

}

/*
Close() makes sure the memory-mapped file has synced its data to the persisted
file and that the persisted file has flushed its contents to stable storage.
Then it truncates the persisted file to the amount of data that’s actually in
it and closes the file.
*/
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = encoding.Uint32(i.mmap[pos : pos+offWidth])
	pos = encoding.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	encoding.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	encoding.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
