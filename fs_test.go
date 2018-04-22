package tarfs

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
)

func TestFromReaderAt(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := tar.NewWriter(buf)

	files := []struct {
		name       string
		mode       os.FileMode
		mod        time.Time
		data       []byte
		numEntries int
	}{
		{"foo", os.ModeDir | 0755, time.Now().Add(-1 * 24 * time.Hour), nil, 3},
		{"foo/bar", 644, time.Now(), []byte{0xa}, 0},
		{"foo/baz", 644, time.Now(), []byte{0xb}, 0},
		{"foo/quux", os.ModeDir | 0755, time.Now().Add(-1 * 23 * time.Hour), nil, 1},
		{"foo/quux/hello", 644, time.Now(), []byte("hello"), 0},
	}

	for _, f := range files {
		if err := w.WriteHeader(newTestHeader(f.name, f.mode, int64(len(f.data)), f.mod)); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(f.data); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()

	db := NewBTreeStore(2)
	rdr := bytes.NewReader(buf.Bytes())
	fs, err := FromReaderAt(rdr, rdr.Size(), db)
	if err != nil {
		t.Fatal(err)
	}

	fCtx := &fuse.Context{}

	rootEntries, status := fs.OpenDir(".", fCtx)
	if !status.Ok() {
		t.Fatal(status)
	}
	if len(rootEntries) != 1 {
		t.Fatalf("expected 1 entry in the root dir, got %d\n%v", len(rootEntries), rootEntries)
	}

	if rootEntries[0].Name != "foo" {
		t.Fatalf("got unexpected root entry name: %s", rootEntries[0].Name)
	}
	_, status = fs.GetAttr(rootEntries[0].Name, fCtx)
	if !status.Ok() {
		t.Fatal(status)
	}

	for _, f := range files {
		if f.mode.IsDir() {
			entries, status := fs.OpenDir(f.name, fCtx)
			if !status.Ok() {
				t.Fatal(status)
			}
			if len(entries) != f.numEntries {
				t.Fatalf("expected %d, got %d\n%+v", f.numEntries, len(entries), entries)
			}

			for _, entry := range entries {
				_, status := fs.GetAttr(filepath.Join(f.name, entry.Name), fCtx)
				if !status.Ok() {
					t.Fatal(status)
				}
			}
		} else {
			file, status := fs.Open(f.name, uint32(os.O_RDONLY), fCtx)
			if !status.Ok() {
				t.Fatal(status)
			}
			buf := make([]byte, len(f.data))
			rr, status := file.Read(buf, 0)
			if !status.Ok() {
				t.Fatal(status)
			}

			if !bytes.Equal(buf[:len(f.data)], f.data) {
				t.Fatal(buf, f.data)
			}
			rr.Done()
		}
	}
}

func newTestHeader(name string, mode os.FileMode, size int64, modTime time.Time) *tar.Header {
	if name != "" && name[len(name)-1] != '/' && mode.IsDir() {
		name += string(os.PathSeparator)
	}
	return &tar.Header{
		Name:    name,
		Size:    size,
		Mode:    int64(mode),
		ModTime: modTime,
	}
}
