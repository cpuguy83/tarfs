package tarfs

import (
	"os"
	"testing"
)

func TestAddGet(t *testing.T) {
	db := NewBTreeStore(2)
	db.Add("/", &node{stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/foo", &node{name: "foo", stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/foo/bar", &node{name: "bar", stat: &StatT{Mode: 644}})

	node := db.Get("/")
	if node == nil {
		t.Fatal("nil node")
	}
	if node.Name() != "" {
		t.Fatalf("got unexpected node for key `/`: %+v", node)
	}

	node = db.Get("/foo")
	if node == nil {
		t.Fatal("nil node")
	}
	if node.Name() != "foo" {
		t.Fatalf("got unexpected node for key `/foo`: %+v", node)
	}

	node = db.Get("/foo/bar")
	if node == nil {
		t.Fatal("nil node")
	}
	if node.Name() != "bar" {
		t.Fatalf("got unexpected node for key `/foo/bar`: %+v", node)
	}

	node = db.Get("/not-exist")
	if node != nil {
		t.Fatalf("expected nil node: %+v", node)
	}
}

func TestEntries(t *testing.T) {
	db := NewBTreeStore(2)
	db.Add("/", &node{stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/foo", &node{name: "foo", stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/bar", &node{name: "bar", stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/bar/baz", &node{name: "baz", stat: &StatT{Mode: 600}})
	db.Add("/bar/quux", &node{name: "quux", stat: &StatT{Mode: 755 | uint32(os.ModeDir)}})
	db.Add("/bar/quux/quack", &node{name: "quack", stat: &StatT{Mode: 600}})

	ls := db.Entries("/")
	if len(ls) != 2 {
		t.Fatalf("expected 2 entries, got: %+v", ls)
	}
	for i, name := range []string{"bar", "foo"} {
		if ls[i].Name() != name {
			t.Fatalf("expected entry %s, got %s", name, ls[i].Name())
		}
	}

	ls = db.Entries("/bar")
	if len(ls) != 2 {
		t.Fatalf("expected 2 entries, got: %+v", ls)
	}
	for i, name := range []string{"baz", "quux"} {
		if ls[i].Name() != name {
			t.Fatalf("expected entry %s, got %s", name, ls[i].Name())
		}
	}

	ls = db.Entries("/foo")
	if len(ls) != 0 {
		t.Fatalf("expected no entries, got: %+v", ls)
	}

	ls = db.Entries("/bar/quux")
	if len(ls) != 1 {
		t.Fatalf("expected 1 entry, got: %+v", ls)
	}
	if ls[0].Name() != "quack" {
		t.Fatalf("expected entry %s, got %+v", "quack", ls[0])
	}
}
