tarfs
======

Tarfs is a library for implementing fuse-based filesystems for tar files.
This is currently purely experimental, **do not use** for real workloads.

## Usage

```go
f, _ := os.Open("foo.tar")
db := tarfs.NewBTreeStore(n)
server := tarfs.FromFile(f, db)
```

See cmd/tarfsd as an example implementation.

## TODO(non-exhaustive):
- Not quite happy with the metadata storage, consider alternatives specifically
around how directory entries are stored and fetched.
- Directory listings don't curently work
