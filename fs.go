package tarfs

import (
	"archive/tar"
	"io"
	"path/filepath"
	"strings"

	"os"

	"github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/pkg/errors"
)

// server is the fuse server which serves a tar file as a path filesystem.
// Currently this server only implements a read-only filesystem.
type server struct {
	pathfs.FileSystem
	db     MetadataStore
	stream io.ReaderAt
}

// Newserver creates a new tarfs server from the passed in metadata store
// The passed in metadata store should be pre-populated with filesystem metadata.
// See `FromFile` as an example of this.
func Newserver(db MetadataStore, tarStream io.ReaderAt) pathfs.FileSystem {
	return &server{
		FileSystem: pathfs.NewReadonlyFileSystem(pathfs.NewDefaultFileSystem()),
		db:         db,
		stream:     tarStream,
	}
}

// FromFile takes the passed in tar file and creates a new tarfs server
// Metadata from the tarfile is stored in the metadata store, which is used as
// the backing store for the tarfs server.
// The passed in file must not be acessed or modified while the server is active.
func FromFile(f *os.File, db MetadataStore) (pathfs.FileSystem, error) {
	tr := tar.NewReader(f)

	for {
		h, err := tr.Next()
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrap(err, "error reading tar")
			}
			break
		}

		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, errors.Wrap(err, "error getting file position in tar")
		}

		var stat StatT
		fillStat(&stat, h.FileInfo())
		stat.Ino = pos

		key := headerNameEntry(h.Name)
		db.Add(key, &node{name: key, stat: &stat})
	}

	return Newserver(db, f), nil
}

func headerNameEntry(name string) string {
	name = strings.TrimSuffix(strings.TrimPrefix(name, "./"), "/")
	if name == "." {
		name = ""
	}
	return fuseNameToKey(name)
}

func fuseNameToKey(name string) string {
	return filepath.Join(string(os.PathSeparator), name)
}

func (s *server) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	logrus.WithField("name", name).Debug("Open")
	f := s.db.Get(fuseNameToKey(name))
	if f == nil {
		return nil, fuse.ENOENT
	}

	return &file{
		ReaderAt: io.NewSectionReader(s.stream, f.Inode(), f.Size()),
		File:     nodefs.NewReadOnlyFile(nodefs.NewDefaultFile()),
		name:     f.Name(),
	}, fuse.OK
}

func fuseMode(fi os.FileInfo) uint32 {
	var mode uint32
	switch {
	case fi.IsDir():
		mode = fuse.S_IFDIR
	case fi.Mode().IsRegular():
		mode = fuse.S_IFREG
	default:
		mode = fuse.S_IFLNK
	}

	mode = mode | uint32(fi.Mode().Perm())
	return mode
}

func (s *server) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	logrus.WithField("name", name).Debug("OpenDir")
	dir := s.db.Get(fuseNameToKey(name))
	if dir == nil {
		return nil, fuse.ENOENT
	}
	if !dir.Mode().IsDir() {
		return nil, fuse.EIO
	}

	if !checkPermissions(dir, context) {
		return nil, fuse.EPERM
	}

	dirEntries := s.db.Entries(fuseNameToKey(name))

	entries := make([]fuse.DirEntry, 0, len(dirEntries))
	for _, e := range dirEntries {
		entries = append(entries, fuse.DirEntry{
			Name: e.Name(),
			Mode: uint32(e.Mode()),
		})
	}

	return entries, fuse.OK
}

func (s *server) GetAttr(name string, context *fuse.Context) (attr *fuse.Attr, status fuse.Status) {
	defer func() {
		logrus.WithField("name", name).WithField("status", status).WithField("attr", attr).Debug("GetAttr")
	}()
	if name == "" {
		return &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Owner: fuse.Owner{Gid: context.Gid, Uid: context.Uid},
		}, fuse.OK
	}
	fi := s.db.Get(fuseNameToKey(name))
	if fi == nil {
		return nil, fuse.ENOENT
	}
	if !checkPermissions(fi, context) {
		return nil, fuse.EPERM
	}

	attr = &fuse.Attr{
		Mtime: uint64(fi.ModTime().Unix()),
		Mode:  uint32(fi.Mode().Perm()),
		Size:  uint64(fi.Size()),
	}
	switch {
	case fi.Mode().IsDir():
		attr.Mode |= fuse.S_IFDIR
	case (fi.Mode() & os.ModeSymlink) == os.ModeSymlink:
		attr.Mode |= fuse.S_IFLNK
	default:
		attr.Mode |= fuse.S_IFREG
	}

	owner := fi.Owner()
	attr.Owner = fuse.Owner{
		Uid: owner.UID,
		Gid: owner.GID,
	}

	return attr, fuse.OK
}

func checkPermissions(fi FileInfo, context *fuse.Context) bool {
	owner := fi.Owner()
	perms := fi.Mode().Perm()

	if perms&(1<<2) != 0 {
		return true
	}
	if owner.GID == context.Owner.Gid {
		return perms&(1<<5) != 0
	}
	if owner.UID == context.Owner.Uid {
		return perms&(1<<8) != 0
	}

	return false
}
