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
		t.Mtime = time.Unix(sys.Mtim.Sec, sys.Mtim.Nsec)
		t.Atime = time.Unix(sys.Atim.Sec, sys.Atim.Nsec)
		t.Ctime = time.Unix(sys.Ctim.Sec, sys.Ctim.Nsec)
	case *syscall.Stat_t:
		t.Owner.UID = sys.Uid
		t.Owner.GID = sys.Gid
		t.Mtime = time.Unix(sys.Mtim.Sec, sys.Mtim.Nsec)
		t.Atime = time.Unix(sys.Atim.Sec, sys.Atim.Nsec)
		t.Ctime = time.Unix(sys.Ctim.Sec, sys.Ctim.Nsec)
	}
}
