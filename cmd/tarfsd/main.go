package main

import (
	"os"
	"os/signal"
	"syscall"

	"fmt"

	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/tarfs"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, usage())
		os.Exit(1)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()

	logrus.SetLevel(logrus.DebugLevel)
	formatter := new(logrus.TextFormatter)
	formatter.FullTimestamp = true
	logrus.SetFormatter(formatter)

	db := tarfs.NewBTreeStore(4)
	tfs, err := tarfs.FromFile(f, db)
	if err != nil {
		panic(err)
	}

	conn := nodefs.NewFileSystemConnector(pathfs.NewPathNodeFs(tfs, nil).Root(), nil)
	srv, err := fuse.NewServer(conn.RawFS(), os.Args[2], &fuse.MountOptions{
		Name: "tarfs",
	})
	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for range c {
			srv.Unmount()
		}
	}()

	srv.Serve()
}

func usage() string {
	return fmt.Sprintf(`Usage:
	%s [TAR FILE PATH] [MOUNT PATH]
`, filepath.Base(os.Args[0]))
}
