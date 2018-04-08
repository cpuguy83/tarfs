package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/tarfs"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {
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
