package tarfs

import (
	"archive/tar"
	"io"
	"path/filepath"
	"strings"

	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// server is the fuse server which serves a tar file as a path filesystem.
// Currently this server only implements a read-only filesystem.
type server struct {
	pathfs.FileSystem
	db     MetadataStore
	stream io.ReaderAt
}

// Newserver creates a new tarfs server from the passed in metadata store.
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
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return FromReaderAt(f, st.Size(), db)
}

// FromReaderAt creates a new tarfs server from io.ReaderAt.
// The size of the tar archive needs to be provided.
// Metadata from the tarfile is stored in the metadata store, which is used as
// the backing store for the tarfs server.
func FromReaderAt(ra io.ReaderAt, size int64, db MetadataStore) (pathfs.FileSystem, error) {
	r := io.NewSectionReader(ra, 0, size)
	tr := tar.NewReader(r)

	// we add the root entry because some archive does not contain the root entry.
	// If the archive contains the real stat for the root, the real stat is used.
	rootStat := StatT{
		Mode: uint32(0755 | os.ModeDir),
		Owner: Owner{
			UID: uint32(os.Geteuid()),
			GID: uint32(os.Getegid()),
		},
		// follows traditional convention, adopted in several file systems
		// including ext4: https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Special_inodes
		Ino:  2,
		Size: 4096,
	}
	rootNode := &dirNode{node: &node{name: "", stat: &rootStat}}
	if err := db.Add("/", rootNode); err != nil {
		return nil, errors.Wrap(err, "error adding root node")
	}

	missingDirs := make(map[string]struct{})
	for {
		h, err := tr.Next()
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrap(err, "error reading tar")
			}
			break
		}

		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, errors.Wrap(err, "error getting file position in tar")
		}

		var stat StatT
		fillStat(&stat, h.FileInfo())
		stat.Ino = pos

		key := headerNameEntry(h.Name)
		var nodeInfo FileInfo = &node{name: h.Name, stat: &stat}
		if h.FileInfo().IsDir() {
			node := nodeInfo.(*node)
			if dirInfo := db.Get(key); dirInfo != nil {
				dirInfo.(*dirNode).node = node
				nodeInfo = dirInfo
				delete(missingDirs, key)
			} else {
				nodeInfo = &dirNode{node: node}
			}
		}

		if err := db.Add(key, nodeInfo); err != nil {
			return nil, errors.Wrapf(err, "error adding node entry to db: %s", h.Name)
		}

		parentKey := filepath.Dir(key)
		var parent *dirNode
		if parentInfo := db.Get(parentKey); parentInfo != nil {
			parent = parentInfo.(*dirNode)
		} else {
			missingDirs[parentKey] = struct{}{}
			parent = &dirNode{node: &node{name: filepath.Base(parentKey)}}
		}
		parent.entries = append(parent.entries, nodeInfo)
		if err := db.Add(parentKey, parent); err != nil {
			return nil, errors.Wrapf(err, "error adding parent node entry to db for %s", h.Name)
		}
	}

	if len(missingDirs) != 0 {
		ss := []string{}
		for s := range missingDirs {
			ss = append(ss, s)
		}
		return nil, errors.Errorf("missing directory entries: %s", strings.Join(ss, ","))
	}

	return Newserver(db, ra), nil
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
			Name: filepath.Base(e.Name()),
			Mode: uint32(e.Mode()),
		})
	}

	return entries, fuse.OK
}

func (s *server) GetAttr(name string, context *fuse.Context) (attr *fuse.Attr, status fuse.Status) {
	logrus.WithField("name", name).Debug("GetAttr")
	defer func() {
		logrus.WithField("name", name).WithField("status", status).WithField("attr", attr).Debug("end GetAttr")
	}()
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

func (s *server) StatFs(name string) *fuse.StatfsOut {
	// TODO: actually fill this in
	// But this is good enough to make this work with overlayfs.
	return &fuse.StatfsOut{}
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
