package tarfs

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func fillStatSys(t *StatT, fi os.FileInfo) {
	switch sys := fi.Sys().(type) {
	case *unix.Stat_t:
		t.Owner.UID = sys.Uid
		t.Owner.GID = sys.Gid
		t.Mtime = time.Unix(sys.Mtimespec.Sec, sys.Mtimespec.Nsec)
		t.Atime = time.Unix(sys.Atimespec.Sec, sys.Atimespec.Nsec)
		t.Ctime = time.Unix(sys.Ctimespec.Sec, sys.Ctimespec.Nsec)
	case *syscall.Stat_t:
		t.Owner.UID = sys.Uid
		t.Owner.GID = sys.Gid
		t.Mtime = time.Unix(sys.Mtimespec.Sec, sys.Mtimespec.Nsec)
		t.Atime = time.Unix(sys.Atimespec.Sec, sys.Atimespec.Nsec)
		t.Ctime = time.Unix(sys.Ctimespec.Sec, sys.Ctimespec.Nsec)
	}
}
