package helpers

import (
	"fmt"
	"sync"
	"testing"

	"github.com/willscott/go-nfs/helpers/memfs"
)

// FromHandle must keep a live handle's ancestors in the cache. They are
// evicted by age like anything else, and a client resolves a file through its
// parents, so an ancestor dropped while a child is still in use surfaces as
// ESTALE on a path that is working.
//
// The share ROOT is the one worth naming: its reverse-index key is the empty
// string, which is what a walk up the path has to end on.
func TestCachingHandlerFromHandleKeepsAncestorsAlive(t *testing.T) {
	mem := memfs.New()
	h := NewCachingHandler(NewNullAuthHandler(mem), 16).(*CachingHandler)

	named := []struct {
		name string
		fh   []byte
	}{
		{"the share root", h.ToHandle(mem, []string{})},
		{"a", h.ToHandle(mem, []string{"a"})},
		{"a/b", h.ToHandle(mem, []string{"a", "b"})},
		{"a/b/c.txt", h.ToHandle(mem, []string{"a", "b", "c.txt"})},
	}
	file := named[len(named)-1].fh

	// Churn well past the cache limit, resolving the deep handle each round.
	// Its ancestors are older than everything the churn adds, so nothing but
	// the refresh keeps them.
	for i := 0; i < 200; i++ {
		if _, _, err := h.FromHandle(file); err != nil {
			t.Fatalf("the file handle went stale at round %d: %v", i, err)
		}
		h.ToHandle(mem, []string{fmt.Sprintf("churn-%d.txt", i)})
	}

	for _, tc := range named {
		if _, _, err := h.FromHandle(tc.fh); err != nil {
			t.Errorf("%s was evicted while a handle below it was in use: %v", tc.name, err)
		}
	}
}

// Two filesystems mint a handle at the same joined path, so ONE reverse-index
// key holds two ids and removing one shifts that slice in place. Every
// FromHandle reads the same key on its way up the path, so the read and the
// shift must not run concurrently. The empty key, the share root, is the one
// every request reaches.
//
// Run with -race: without it this test passes on a torn read.
func TestCachingHandlerFromHandleAndInvalidateShareAReverseKey(t *testing.T) {
	first, second := memfs.New(), memfs.New()
	h := NewCachingHandler(NewNullAuthHandler(first), 1024).(*CachingHandler)

	h.ToHandle(first, []string{})
	file := h.ToHandle(first, []string{"a", "b.txt"})

	const rounds = 500
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if _, _, err := h.FromHandle(file); err != nil {
				t.Errorf("resolving the file at round %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			// Lands on the same key as the first filesystem's root.
			root := h.ToHandle(second, []string{})
			_ = h.InvalidateHandle(second, root)
		}
	}()
	wg.Wait()
}
