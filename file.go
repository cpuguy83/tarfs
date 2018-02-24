package tarfs

import (
	"archive/tar"
	"io"
	"time"

	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/pkg/errors"
)

// FileInfo is the metadata stored about a node in a tar file.
type FileInfo interface {
	ModTime() time.Time
	AccessTime() time.Time
	ChangeTime() time.Time
	Inode() int64
	Size() int64
	Mode() os.FileMode
	Owner() Owner
	Name() string
}

// Owner is the uid/gid used for a filesystem node
type Owner struct {
	UID uint32
	GID uint32
}

// StatT is an implementation of FileInfo.
// TODO: Do we need `FileInfo`?
type StatT struct {
	Mode  uint32
	Owner Owner
	Atime time.Time
	Mtime time.Time
	Ctime time.Time
	Ino   int64
	Size  int64
}

type dirNode struct {
	node
	entries []FileInfo
}

func (n *dirNode) Entries() []FileInfo {
	entries := make([]FileInfo, 0, len(n.entries))
	for _, e := range n.entries {
		entries = append(entries, e)
	}
	return entries
}

type fileNode struct {
	node
}

type node struct {
	name string
	stat *StatT
}

func (n *node) Name() string {
	return n.name
}

func (n *node) Size() int64 {
	return n.stat.Size
}

func (n *node) Sys() interface{} {
	return n.stat
}

func (n *node) ModTime() time.Time {
	return n.stat.Mtime
}

func (n *node) ChangeTime() time.Time {
	return n.stat.Mtime
}

func (n *node) AccessTime() time.Time {
	return n.stat.Atime
}

func (n *node) Mode() os.FileMode {
	return os.FileMode(n.stat.Mode)
}

func (n *node) Inode() int64 {
	return n.stat.Ino
}

func (n *node) IsDir() bool {
	return n.Mode()&fuse.S_IFDIR > 0 || n.Mode().IsDir()
}

func (n *node) Owner() Owner {
	return n.stat.Owner
}

type file struct {
	name string
	io.ReaderAt
	nodefs.File
}

func (f *file) String() string {
	return f.name
}

func (f *file) Read(p []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := f.ReadAt(p, off)

	var status fuse.Status
	var rr fuse.ReadResult

	switch errors.Cause(err) {
	case nil:
		status = fuse.OK
		rr = fuse.ReadResultData(p)
	case io.EOF:
		status = fuse.OK
		if n <= 0 {
			rr = eofReadResult{}
		} else {
			rr = fuse.ReadResultData(p)
		}
	default:
		status = fuse.EIO
	}
	return rr, status
}

type eofReadResult struct{}

func (eofReadResult) Size() int {
	return 0
}

func (eofReadResult) Bytes(b []byte) ([]byte, fuse.Status) {
	return nil, fuse.OK
}

func (eofReadResult) Done() {}

func fillStat(t *StatT, fi os.FileInfo) {
	fillStatSys(t, fi)

	switch sys := fi.Sys().(type) {
	case *tar.Header:
		t.Atime = sys.AccessTime
		t.Ctime = sys.ChangeTime
		t.Owner.UID = uint32(sys.Uid)
		t.Owner.GID = uint32(sys.Gid)

	}

	t.Mode = uint32(fi.Mode())
	t.Size = fi.Size()
	t.Mtime = fi.ModTime()
}
