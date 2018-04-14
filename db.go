package tarfs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/btree"
	"github.com/sirupsen/logrus"
)

// MetadataStore is an abstraction for implementing different storages for
// filesystem metadata.
// This is used to insert or delete file metadata based on a key (typically the file path).
// It is also used to get the entries for a particular directory.
// TODO: Maybe `Entries()` isn't right here
type MetadataStore interface {
	Get(string) FileInfo
	Add(string, FileInfo) error
	Entries(string) []FileInfo
}

// stringKey is used to wrap FileInfo metadata and sort keys for the B-Tree.
type stringKey struct {
	key  string
	info FileInfo
}

func (k *stringKey) Less(other btree.Item) bool {
	keyCount := strings.Count(k.key, "/")
	oKey := other.(*stringKey)
	oCount := strings.Count(oKey.key, "/")

	if keyCount < oCount {
		return true
	}
	if keyCount == oCount {
		return k.key < oKey.key
	}
	return false
}

// NewBTreeStore creates a nw MetadatStore backed by an in-memory b-tree of the
// passed in degree.
func NewBTreeStore(degree int) MetadataStore {
	return &btreeStore{
		db: btree.New(degree),
	}
}

type btreeStore struct {
	db *btree.BTree
}

func (s *btreeStore) Add(key string, fi FileInfo) error {
	logrus.WithField("key", key).WithField("info", fi).Debug("store.Add")
	sk := &stringKey{
		key:  key,
		info: fi,
	}
	s.db.ReplaceOrInsert(sk)
	return nil
}

func (s *btreeStore) Get(key string) FileInfo {
	logrus.WithField("key", key).Debug("store.Get")
	var info FileInfo
	defer logrus.WithField("info", fmt.Sprintf("+%v", info)).Debug("end store.Get")

	sk := &stringKey{
		key: key,
	}
	i := s.db.Get(sk)
	if i == nil {
		return nil
	}
	info = i.(*stringKey).info
	return info
}

// DirIndex is an interface which can be implemented by a FileInfo for the purpose
// of retreiving directory entries directly from the dir node.
type DirIndex interface {
	Entries() []FileInfo
}

func (s *btreeStore) Entries(key string) []FileInfo {
	logrus.WithField("key", key).Debug("Entries")
	defer logrus.WithField("key", key).Debug("end Entries")

	i := s.db.Get(&stringKey{key: key})
	if i == nil {
		panic("non-existent key")
	}
	sk := i.(*stringKey)
	if !sk.info.Mode().IsDir() {
		panic("cannot list entries for non-dir: " + sk.info.Name())
	}
	if idx, ok := sk.info.(DirIndex); ok {
		return idx.Entries()
	}

	until := &stringKey{
		key: sk.key + "//",
	}

	logrus.WithField("key", key).Debug("performing btree search for dir entries")
	entries := make([]FileInfo, 0, 5)
	s.db.AscendRange(sk, until, func(i btree.Item) bool {
		esk := i.(*stringKey)
		if esk.key == key {
			return true
		}
		logrus.WithField("parent key", key).WithField("entry key", esk.key).Debug("ascend range")
		if filepath.Dir(esk.key) == key {
			entries = append(entries, esk.info)
		}
		return true
	})
	return entries
}
