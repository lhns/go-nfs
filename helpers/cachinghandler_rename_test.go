package helpers

import (
	"reflect"
	"testing"

	"github.com/willscott/go-nfs/helpers/memfs"
)

// TestCachingHandlerRenameKeepsHandle pins that Rename re-points an existing
// handle at the new path instead of invalidating it: a handle taken for the old
// path must resolve to the new path afterwards, so a client holding the file
// open does not get ESTALE.
func TestCachingHandlerRenameKeepsHandle(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cache := NewCachingHandler(handler, 1024).(*CachingHandler)

	oldHandle := cache.ToHandle(mem, []string{"old.txt"})

	if err := cache.Rename(mem, []string{"old.txt"}, mem, []string{"new.txt"}); err != nil {
		t.Fatal(err)
	}

	_, path, err := cache.FromHandle(oldHandle)
	if err != nil {
		t.Fatalf("handle went stale after rename: %v", err)
	}
	if !reflect.DeepEqual(path, []string{"new.txt"}) {
		t.Fatalf("handle resolves to %v, want [new.txt]", path)
	}

	// The new path resolves to the same handle (reverse cache moved too).
	if got := cache.ToHandle(mem, []string{"new.txt"}); !reflect.DeepEqual(got, oldHandle) {
		t.Fatalf("ToHandle(new) = %x, want the moved handle %x", got, oldHandle)
	}
}

// TestCachingHandlerRenameMovesDescendants pins that renaming a directory moves
// the handles of files open inside it, so a descriptor held across a directory
// rename stays valid.
func TestCachingHandlerRenameMovesDescendants(t *testing.T) {
	mem := memfs.New()
	handler := NewNullAuthHandler(mem)
	cache := NewCachingHandler(handler, 1024).(*CachingHandler)

	childHandle := cache.ToHandle(mem, []string{"dir", "f.txt"})

	if err := cache.Rename(mem, []string{"dir"}, mem, []string{"dir2"}); err != nil {
		t.Fatal(err)
	}

	_, path, err := cache.FromHandle(childHandle)
	if err != nil {
		t.Fatalf("descendant handle went stale after dir rename: %v", err)
	}
	if !reflect.DeepEqual(path, []string{"dir2", "f.txt"}) {
		t.Fatalf("descendant resolves to %v, want [dir2 f.txt]", path)
	}
}
